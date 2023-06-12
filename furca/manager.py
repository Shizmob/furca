import os
import signal
from errno import ECHILD
from time import monotonic
from enum import Enum
from threading import main_thread, current_thread
from selectors import BaseSelector, DefaultSelector, EVENT_READ
from logging import getLogger

from types import FrameType
from typing import Optional as O, Type, Tuple, List, Dict, Callable, Any, TypeVar, Generic, Iterable
from typing_extensions import TypeAlias, Self

from .resource import Resource, CreatedResources
from .comm import PipeEvent, PipeWaiter, PipeNotifier, PipePortal
from .worker import Worker

logger = getLogger(__name__)

class HookType(Enum):
    PreFork = "pre-fork"
    PostFork = "post-fork"

Hook: TypeAlias = Callable[[], None]

GLOBAL_HOOKS: Dict[HookType, List[Hook]] = {hook_type: [] for hook_type in HookType}

def pre_fork(hook: Hook) -> Hook:
    GLOBAL_HOOKS[HookType.PreFork].append(hook)
    return hook

def post_fork(hook: Hook) -> Hook:
    GLOBAL_HOOKS[HookType.PostFork].append(hook)
    return hook

class WorkerStatus:
    waiter: O[PipeWaiter]
    pid: int
    timeout: float
    kill_timeout: float
    last_activity: float
    stopping: bool

    def __init__(
        self,
        waiter: PipeWaiter,
        pid: int,
        timeout: float = 5.0,
        kill_timeout: float = 5.0,
    ) -> None:
        self.waiter = waiter
        self.pid = pid
        self.alive_timeout = timeout
        self.kill_timeout = kill_timeout
        self.reset()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if self.waiter:
            self.waiter.close()
            self.waiter = None

    def closed(self) -> bool:
        return self.waiter is None

    def reset(self) -> None:
        if self.waiter:
            self.waiter.clear()
        self.last_activity = monotonic()
        self.stopping = False

    def ping(self) -> bool:
        assert self.waiter is not None
        if self.waiter.is_set():
            if not self.stopping:
                self.last_activity = monotonic()
            self.waiter.clear()
        return not self.waiter.closed

    def stop(self) -> None:
        if self.stopping:
            os.kill(self.pid, signal.SIGKILL)
        else:
            os.kill(self.pid, signal.SIGTERM)
            self.stopping = True
            self.last_activity = monotonic()

    def kill(self) -> None:
        self.stopping = True
        self.stop()

    def is_expired(self) -> bool:
        return monotonic() - self.last_activity > (self.kill_timeout if self.stopping else self.alive_timeout)


WorkerT = TypeVar('WorkerT', bound=Worker)
WorkerFn: TypeAlias = Callable[[PipeNotifier, float, CreatedResources], WorkerT]

class Manager(Generic[WorkerT]):
    worker: WorkerFn[WorkerT]
    worker_count: int
    workers: Dict[int, WorkerStatus]
    alive_timeout: float
    kill_timeout: float
    resources: CreatedResources
    fallthrough: bool

    handled_signals: Dict[int, Any]
    stop_event: O[PipeEvent]
    child_event: O[PipePortal[Tuple[int, int]]]

    def __init__(self, worker: WorkerFn[WorkerT], count: O[int], resources: Iterable[Resource[Any]] = [], alive_timeout: float = 5.0, kill_timeout: float = 5.0, fallthrough: bool = True) -> None:
        self.worker = worker
        self.worker_count = count or os.cpu_count() or 1
        self.workers = {}
        self.alive_timeout = alive_timeout
        self.kill_timeout = kill_timeout
        self.resources = {r: r.create() for r in resources}
        self.fallthrough = fallthrough

        self.handled_signals = {}
        self.stop_event = None
        self.child_event = None

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: O[Type[BaseException]], exc_value: O[BaseException], traceback: O[FrameType]) -> O[bool]:
        self.close()
        return None

    def close(self) -> None:
        if self.stop_event:
            self.stop_event.close()
            self.stop_event = None
        if self.child_event:
            self.child_event.close()
            self.child_event = None

    def spawn_worker(self) -> WorkerStatus:
        for hook in GLOBAL_HOOKS[HookType.PreFork]:
            hook()

        waiter, notifier = PipeEvent().split()
        waiter.set_inheritable(False)
        notifier.set_inheritable(True)

        pid = os.fork()
        if pid == 0:
            for status in self.workers.values():
                status.close()
            waiter.close()
            self.workers = {}
            self.worker_count = 0
            for sig in self.handled_signals:
                signal.signal(sig, signal.SIG_DFL)
            notifier.set()
            worker = self.worker(notifier, 0.4 * self.alive_timeout, self.resources)
            for hooks in GLOBAL_HOOKS[HookType.PostFork]:
                hook()
            try:
                worker.run()
                raise SystemExit(0)
            except SystemExit as e:
                if self.fallthrough:
                    raise
                os._exit(e.code if isinstance(e.code, int) else 1)
            except BaseException:
                if self.fallthrough:
                    raise
                logger.exception('[worker %d] exception', os.getpid())
                os._exit(255)

        waiter.wait()
        waiter.clear()
        notifier.set_inheritable(False)
        notifier.close()

        status = WorkerStatus(waiter, pid, self.alive_timeout, self.kill_timeout)
        self.workers[pid] = status
        return status

    def stop_worker(self, status: O[WorkerStatus] = None) -> None: 
        if status is None:
            if self.workers:
                status = next(iter(self.workers.values()))
        if status is None:
            return
        status.stop()

    def remove_worker(self, status: WorkerStatus) -> bool:
        if not self.has_worker(status):
            return False

        del self.workers[status.pid]
        status.close()
        del status
        return True

    def has_worker(self, status: WorkerStatus) -> bool:
        return self.workers.get(status.pid, None) == status

    def handle_child_sig(self, value: int, frame: O[FrameType]) -> None:
        pids = []
        while True:
            try:
                (pid, status) = os.waitpid(-1, os.WNOHANG)
            except OSError as e:
                if e.errno == ECHILD:
                    break
            if pid == 0:
                break
            pids.append((pid, status))
        if self.child_event:
            self.child_event.extend(pids)

    def handle_stop_sig(self, value: int, frame: O[FrameType]) -> None:
        if self.stop_event:
            self.stop_event.set()

    def setup(self) -> None:
        self.setup_signal(signal.SIGCHLD, self.handle_child_sig)
        self.setup_signal(signal.SIGINT, self.handle_stop_sig)
        self.setup_signal(signal.SIGHUP, self.handle_stop_sig)
        self.setup_signal(signal.SIGTERM, self.handle_stop_sig)

    def setup_signal(self, sig: int, handler: Any) -> None:
        if signal.getsignal(sig) == handler:
            return
        if current_thread() != main_thread():
            raise RuntimeError("can not initialize from non-main thread! please call Manager.setup() from main thread")
        signal.signal(sig, handler)
        self.handled_signals[sig] = handler

    def run(self, periodics: List[Callable[[], O[bool]]] = []) -> None:
        self.setup()

        if not self.stop_event:
            self.stop_event = PipeEvent()
        if not self.child_event:
            self.child_event = PipePortal()

        stop_event = self.stop_event
        child_event = self.child_event
        stop_event.clear()
        try:
            selector = DefaultSelector()
            selector.register(child_event, EVENT_READ, child_event)
            selector.register(stop_event, EVENT_READ, stop_event)
            should_stop = False

            def check_worker_count() -> None:
                if not should_stop:
                    while len(self.workers) > self.worker_count:
                        self.stop_worker()
                    while len(self.workers) < self.worker_count:
                        status = self.spawn_worker()
                        status.reset()
                        if status.waiter:
                            selector.register(status.waiter, EVENT_READ, status)

            def check_worker_timeout() -> None:
                for status in self.workers.values():
                    if (should_stop and not status.stopping) or status.is_expired():
                        if not should_stop:
                            logger.warning('[worker %d] timeout', status.pid)
                        self.stop_worker(status)

            periodics = periodics + [check_worker_count, check_worker_timeout]

            while not should_stop or self.workers:
                for p in periodics:
                    if p():
                        should_stop = True

                for key, _ in selector.select(1.0):
                    if key.data == stop_event:
                        stop_event.clear()
                        should_stop = True
                    elif key.data == child_event:
                        while True:
                            st = child_event.next()
                            if st is None:
                                break
                            pid, s = st
                            if pid in self.workers:
                                status = self.workers[pid]
                                if not status.stopping:
                                    logger.warning('[worker %d] exited: %d', pid, s)
                                if status.waiter and not status.closed():
                                    selector.unregister(status.waiter)
                                self.remove_worker(status)
                    elif isinstance(key.data, WorkerStatus):
                        status = key.data
                        if self.has_worker(status):
                            if not status.ping():
                                logger.debug('[worker %d] notify fd closed:', status.pid)
                                if status.waiter:
                                    selector.unregister(status.waiter)
                                status.close()
        finally:
            del stop_event, child_event
            selector.close()
            for status in self.workers.values():
                try:
                    status.kill()
                except:
                    pass
                status.close()
            self.workers.clear()

    def stop(self) -> None:
        if self.stop_event:
            self.stop_event.set()

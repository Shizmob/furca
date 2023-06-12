import os
import signal
import anyio
from time import sleep

from threading import Thread
from typing import List, Any, NoReturn, Protocol, TypeVar, Generic

from .comm import PipeNotifier
from .resource import ResourceWithValue


class Worker:
    notifier: PipeNotifier
    resources: List[ResourceWithValue[Any]]
    interval: float

    def __init__(
        self,
        notifier: PipeNotifier,
        interval: float,
        resources: List[ResourceWithValue[Any]] = [],
    ) -> None:
        self.notifier = notifier
        self.interval = interval
        self.resources = resources

    def run(self) -> None:
        pass

class SyncWorkFn(Protocol):
    def __call__(self, resources: List[ResourceWithValue[Any]]) -> None:
        ...

class SyncWorker(Worker):
    fn: SyncWorkFn

    def __init__(self, fn: SyncWorkFn, notifier: PipeNotifier, interval: float, resources: List[ResourceWithValue[Any]] = []) -> None:
        super().__init__(notifier, interval, resources)
        self.fn = fn

    def handle_channel(self) -> None:
        while True:
            sleep(self.interval)
            try:
                self.notifier.set()
            except OSError:
                os.kill(os.getpid(), signal.SIGTERM)
                break

    def run(self) -> None:
        Thread(target=self.handle_channel, daemon=True).start()
        self.fn(self.resources)

class AsyncWorkFn(Protocol):
    async def __call__(self, resources: List[ResourceWithValue[Any]]) -> None:
        ...

class AsyncWorker(Worker):
    fn: AsyncWorkFn

    def __init__(self, fn: AsyncWorkFn, notifier: PipeNotifier, interval: float, resources: List[ResourceWithValue[Any]]) -> None:
        super().__init__(notifier, interval, resources)
        self.fn = fn

    async def handle_signals(self, scope: anyio.CancelScope) -> None:
        with anyio.open_signal_receiver(signal.SIGHUP, signal.SIGTERM, signal.SIGINT) as signals:
            async for sig in signals:
                scope.cancel()

    async def handle_channel(self, scope: anyio.CancelScope) -> None:
        while True:
            try:
                self.notifier.set()
            except OSError as e:
                scope.cancel()
                return
            await anyio.sleep(self.interval)

    async def handle_worker(self, scope: anyio.CancelScope) -> None:
        await self.fn(self.resources)
        scope.cancel()
        return

    async def loop(self) -> None:
        async with anyio.create_task_group() as tg:
            tg.start_soon(self.handle_signals, tg.cancel_scope)
            tg.start_soon(self.handle_channel, tg.cancel_scope)
            tg.start_soon(self.handle_worker, tg.cancel_scope)

    def run(self) -> None:
        anyio.run(self.loop)


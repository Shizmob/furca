from os import set_blocking, set_inheritable, pipe, read, write, close
from time import monotonic
from select import select
from errno import EBADF
from selectors import DefaultSelector, SelectorKey, EVENT_READ

from typing import (
    Optional as O,
    Union as U,
    Any,
    Tuple,
    List,
    Dict,
    Iterable,
    Generic,
    TypeVar,
)

class Deadline:
    def __init__(self, timeout: U[float, int]) -> None:
        self.deadline = monotonic() + timeout

    def passed(self) -> bool:
        return self.remaining() == 0.0

    def remaining(self) -> float:
        return max(self.deadline - monotonic(), 0.0)

class PipeNotifier:
    __slots__ = ("fd", )
    fd: O[int]

    def __init__(self, fd: int) -> None:
        self.fd = fd
        set_blocking(self.fd, True)

    def __del__(self) -> None:
        if self.fd is not None:
            self.close()

    def set_inheritable(self, value: bool) -> None:
        assert self.fd is not None
        set_inheritable(self.fd, value)

    def close(self) -> None:
        assert self.fd is not None
        try:
            close(self.fd)
        except OSError as e:
            if e.errno != EBADF:
                raise
        self.fd = None

    def set(self) -> None:
        assert self.fd is not None
        write(self.fd, b"x")

    def fileno(self) -> int:
        assert self.fd is not None
        return self.fd

class PipeWaiter:
    __slots__ = ("fd", "closed", "value")
    fd: O[int]
    closed: bool
    value: int

    def __init__(self, fd: int) -> None:
        self.fd = fd
        self.closed = False
        self.value = 0
        set_blocking(self.fd, False)

    def __del__(self) -> None:
        if self.fd is not None:
            self.close()

    def set_inheritable(self, value: bool) -> None:
        assert self.fd is not None
        set_inheritable(self.fd, value)

    def close(self) -> None:
        assert self.fd is not None
        try:
            close(self.fd)
        except OSError as e:
            if e.errno != EBADF:
                raise
        self.fd = None
        self.closed = True

    def wait(self, timeout: O[U[float, int]] = None) -> bool:
        deadline = Deadline(timeout) if timeout else None
        while not self.closed and not self.value and (not deadline or not deadline.passed()):
            if self.fd is None:
                return False
            r, _, _ = select([self.fd], [], [], deadline.remaining() if deadline else timeout)
            if not r:
                if timeout == 0:
                    break
                else:
                    continue
            b = read(self.fd, 64)
            self.value += len(b)
            if not b:
                self.closed = True

        return self.value > 0

    def is_set(self) -> bool:
        return self.wait(timeout=0)

    def clear(self) -> None:
        self.value = 0
        while self.wait(timeout=0):
            self.value = 0

    def fileno(self) -> int:
        assert self.fd is not None
        return self.fd

class PipeEvent:
    __slots__ = ("waiter", "notifier")
    waiter: O[PipeWaiter]
    notifier: O[PipeNotifier]

    def __init__(self) -> None:
        r, w = pipe()
        self.waiter = PipeWaiter(r)
        self.notifier = PipeNotifier(w)

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if self.waiter:
            self.waiter.close()
            self.waiter = None
        if self.notifier:
            self.notifier.close()
            self.notifier = None

    def split(self) -> Tuple[PipeWaiter, PipeNotifier]:
        if not self.waiter or not self.notifier:
            raise RuntimeError(f"{self} is closed")
        waiter, notifier = self.waiter, self.notifier
        self.waiter = None
        self.notifier = None
        return waiter, notifier

    def is_set(self) -> bool:
        if not self.waiter:
            raise RuntimeError(f"{self} is closed")
        return self.waiter.is_set()

    def set(self) -> None:
        if not self.notifier:
            raise RuntimeError(f"{self} is closed")
        return self.notifier.set()

    def clear(self) -> None:
        if not self.waiter:
            raise RuntimeError(f"{self} is closed")
        self.waiter.clear()

    def wait(self, timeout: O[U[float, int]] = None) -> bool:
        if not self.waiter:
            raise RuntimeError(f"{self} is closed")
        return self.waiter.wait(timeout)

    def fileno(self) -> int:
        if not self.waiter:
            raise RuntimeError(f"{self} is closed")
        return self.waiter.fileno()

    @property
    def closed(self) -> bool:
        return not self.waiter or self.waiter.closed

T = TypeVar("T")

class PipePortal(Generic[T]):
    event: PipeEvent
    list: list[T]

    def __init__(self) -> None:
        self.event = PipeEvent()
        self.list = []

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        self.event.close()

    def append(self, value: T) -> None:
        self.list.append(value)
        self.event.set()

    def extend(self, values: Iterable[T]) -> None:
        self.list.extend(values)
        self.event.set()

    def fileno(self) -> int:
        return self.event.fileno()

    def next(self) -> O[T]:
        try:
            value = self.list.pop(0)
        except IndexError:
            return None
        if not self.list:
            self.event.clear()
        return value

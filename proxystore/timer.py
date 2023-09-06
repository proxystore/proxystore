"""Timing utilities."""
from __future__ import annotations

import sys
import time
from types import TracebackType

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


class Timer:
    """Performance timer with nanosecond precision.

    Example:
        ```python
        from proxystore.timer import Timer

        with Timer() as timer:
            ...

        print(timer.elapsed_ms)
        ```

    Example:
        ```python
        from proxystore.timer import Timer

        timer = Timer()
        timer.start()
        ...
        timer.stop()

        print(timer.elapsed_ms)
        ```

    Raises:
        RuntimeError: If the elapsed time is accessed before the timer is
            stopped or the context block is exited.
    """

    def __init__(self) -> None:
        self._start = 0
        self._end = 0
        self._running = False

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.stop()

    @property
    def elapsed_ns(self) -> int:
        """Elapsed time in nanoseconds."""
        if self._running:
            raise RuntimeError('Timer is still running!')
        return self._end - self._start

    @property
    def elapsed_ms(self) -> float:
        """Elapsed time in milliseconds."""
        return self.elapsed_ns / 1e6

    @property
    def elapsed_s(self) -> float:
        """Elapsed time in seconds."""
        return self.elapsed_ns / 1e9

    def start(self) -> None:
        """Start the timer."""
        self._running = True
        self._start = time.perf_counter_ns()

    def stop(self) -> None:
        """Stop the timer."""
        self._end = time.perf_counter_ns()
        self._running = False

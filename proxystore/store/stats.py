"""Utilities for Tracking Stats on Store Operations."""
from __future__ import annotations

import math
from collections.abc import MutableMapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any
from typing import Callable
from typing import cast
from typing import Iterable
from typing import Iterator
from typing import KeysView
from typing import NamedTuple
from typing import TypeVar


FuncType = TypeVar("FuncType", bound=Callable[..., Any])


class Event(NamedTuple):
    """Event corresponding to a function called with a specific key."""

    function: str
    key: str


@dataclass
class TimeStats:
    """Helper class for tracking time stats of an operation."""

    calls: int = 0
    avg_time_ms: float = 0
    min_time_ms: float = math.inf
    max_time_ms: float = 0

    def __add__(self, other: TimeStats) -> TimeStats:
        """Add two instances together."""
        return TimeStats(
            calls=self.calls + other.calls,
            avg_time_ms=self._weighted_avg(
                self.avg_time_ms,
                self.calls,
                other.avg_time_ms,
                other.calls,
            ),
            min_time_ms=min(self.min_time_ms, other.min_time_ms),
            max_time_ms=max(self.max_time_ms, other.max_time_ms),
        )

    def add_time(self, time_ms: float) -> None:
        """Add a new time to the stats.

        Args:
            time_ms (float): time (milliseconds) of a method execution.
        """
        self.avg_time_ms = self._weighted_avg(
            self.avg_time_ms,
            self.calls,
            time_ms,
            1,
        )
        self.min_time_ms = min(time_ms, self.min_time_ms)
        self.max_time_ms = max(time_ms, self.max_time_ms)
        self.calls += 1

    def _weighted_avg(self, a1: float, n1: int, a2: float, n2: float) -> float:
        """Compute weighted average between two separate averages.

        Args:
            a1 (float): first average.
            n1 (float): number of samples in `a1`.
            a2 (float): second average.
            n2 (float): number of samples in `a2`.

        Returns:
            weighted average between `a1` and `a2`.
        """
        return ((a1 * n1) + (a2 * n2)) / (n1 + n2)


class FunctionEventStats(MutableMapping):
    """Class for tracking stats of calls of functions that take a key."""

    def __init__(self) -> None:
        """Init FunctionEventStats."""
        self._events: dict[Event, TimeStats] = {}

    def __delitem__(self, event: Event) -> None:
        """Remove event from self."""
        del self._events[event]

    def __getitem__(self, event: Event) -> TimeStats:
        """Get item corresponding to event."""
        if not isinstance(event, Event):
            raise TypeError(
                f"key (event) must be of type {Event.__name__}. "
                f"Got type {type(event)}.",
            )
        if event not in self._events:
            self._events[event] = TimeStats()
        return self._events[event]

    def __iter__(self) -> Iterator[Any]:
        """Get an iterator of events."""
        return iter(self._events)

    def __len__(self) -> int:
        """Get number of tracked events."""
        return len(self._events)

    def __setitem__(self, event: Event, stats: TimeStats) -> None:
        """Set stats for event."""
        if not isinstance(event, Event):
            raise TypeError(
                f"key (event) must be of type {Event.__name__}. "
                f"Got type {type(event)}.",
            )
        if not isinstance(stats, TimeStats):
            raise TypeError(
                f"value (stats) must be of type {TimeStats.__name__}. "
                f"Got type {type(stats)}.",
            )
        self._events[event] = stats

    def keys(self) -> KeysView[Any]:
        """Returns list of events being tracked."""
        return self._events.keys()

    def update(self, iterable: Iterable[Any]) -> None:  # type: ignore
        """Update self from a dict or iterable of items."""
        if isinstance(iterable, dict):
            iterable = iterable.items()
        for event, stats in iterable:
            self[event] += stats

    def wrap(self, function: FuncType, key_is_kwarg: bool = False) -> FuncType:
        """Wraps a method to log stats on calls to the function.

        Args:
            function (callable): function to wrap.
            key_is_kwarg (bool): the key passed to `function` is assumed to be
                the first positional arg. If the key is passed
                as a kwarg, set this to `True` (default: False).

        Returns:
            callable with same interface as method.
        """

        def _function(*args: Any, **kwargs: Any) -> Any:
            if key_is_kwarg and "key" in kwargs:
                key = kwargs["key"]
            elif not key_is_kwarg and len(args) > 0:
                key = args[0]
            else:
                key = None
            event = Event(function=function.__name__, key=key)

            start = perf_counter()
            result = function(*args, **kwargs)
            time = perf_counter() - start

            self[event].add_time(time * 1000)
            return result

        return cast(FuncType, _function)

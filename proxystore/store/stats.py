"""Utilities for Tracking Stats on Store Operations."""
from __future__ import annotations

import math
from collections.abc import MutableMapping
from dataclasses import dataclass
from functools import partial
from time import perf_counter_ns
from typing import Any
from typing import Callable
from typing import cast
from typing import Iterator
from typing import KeysView
from typing import NamedTuple
from typing import TypeVar

import proxystore as ps

GenericCallable = TypeVar('GenericCallable', bound=Callable[..., Any])


STORE_METHOD_KEY_IS_RESULT = {
    'evict': False,
    'exists': False,
    'get': False,
    'get_bytes': False,
    'is_cached': False,
    'proxy': True,
    'set': True,
    'set_bytes': False,
}


class Event(NamedTuple):
    """Event corresponding to a function called with a specific key."""

    function: str
    key: str | None


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


class FunctionEventStats(MutableMapping):  # type: ignore
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
                f'key (event) must be of type {Event.__name__}. '
                f'Got type {type(event)}.',
            )
        if event not in self._events:
            self._events[event] = TimeStats()
        return self._events[event]

    def __iter__(self) -> Iterator[Event]:
        """Get an iterator of events."""
        return iter(self._events)

    def __len__(self) -> int:
        """Get number of tracked events."""
        return len(self._events)

    def __setitem__(self, event: Event, stats: TimeStats) -> None:
        """Set stats for event."""
        if not isinstance(event, Event):
            raise TypeError(
                f'key (event) must be of type {Event.__name__}. '
                f'Got type {type(event)}.',
            )
        if not isinstance(stats, TimeStats):
            raise TypeError(
                f'value (stats) must be of type {TimeStats.__name__}. '
                f'Got type {type(stats)}.',
            )

        if event in self._events:
            self._events[event] += stats
        else:
            self._events[event] = stats

    def keys(self) -> KeysView[Event]:
        """Returns list of events being tracked."""
        return self._events.keys()

    def _function(
        self,
        function: GenericCallable,
        key_is_result: bool,
        preset_key: str | None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a wrapped function and store execution stats.

        Args:
            function (callable): function to wrap.
            key_is_result (bool): if `True`, the key is the return value of
                `function` rather than the first argument. (default: False).
            preset_key (str): optionally preset the key associated with
                any calls to `function`. This overrides `key_is_returned`.
            args: Arguments passed to `function`
            kwargs: Keywords arguments passed to `function`.

        Returns:
            Output of the function
        """
        start_ns = perf_counter_ns()
        result = function(*args, **kwargs)
        time_ns = perf_counter_ns() - start_ns

        if key_is_result:
            if isinstance(result, ps.proxy.Proxy):
                key = ps.proxy.get_key(result)
            else:
                key = result
        elif preset_key is not None:
            key = preset_key
        elif len(args) > 0:
            key = args[0]
        else:
            key = None

        event = Event(function=function.__name__, key=key)
        self[event].add_time(time_ns / 1e6)

        return result

    def wrap(
        self,
        function: GenericCallable,
        *,
        key_is_result: bool = False,
        preset_key: str | None = None,
    ) -> GenericCallable:
        """Wraps a method to log stats on calls to the function.

        Args:
            function (callable): function to wrap.
            key_is_result (bool): if `True`, the key is the return value of
                `function` rather than the first argument. (default: False).
            preset_key (str): optionally preset the key associated with
                any calls to `function`. This overrides `key_is_returned`.

        Returns:
            callable with same interface as `function`.
        """
        out_fun = partial(self._function, function, key_is_result, preset_key)

        return cast(GenericCallable, out_fun)

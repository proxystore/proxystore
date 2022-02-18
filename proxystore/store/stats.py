from __future__ import annotations

import math
from collections import defaultdict
from time import perf_counter
from typing import Any
from typing import Callable
from typing import cast
from typing import TypeVar


FuncType = TypeVar("FuncType", bound=Callable[..., Any])


class _FunctionStats:
    """Helper class for tracking stats of an individual function."""

    def __init__(self) -> None:
        """Init _FunctionStats."""
        self._calls: int = 0
        self._total_time: float = 0
        self._min_time: float = math.inf
        self._max_time: float = 0

    def __add__(self, other_stats: _FunctionStats) -> _FunctionStats:
        """Add two instances together."""
        new_stats = _FunctionStats()
        new_stats._calls = self._calls + other_stats._calls
        new_stats._total_time = self._total_time + other_stats._total_time
        new_stats._min_time = min(self._min_time, other_stats._min_time)
        new_stats._max_time = max(self._max_time, other_stats._max_time)
        return new_stats

    def add_time(self, time: float) -> None:
        """Add a new time to the stats.

        Args:
            time (float): time of a method execution.
        """
        self._calls += 1
        self._total_time += time
        self._min_time = min(time, self._min_time)
        self._max_time = max(time, self._max_time)

    def as_dict(self) -> dict[str, int | float]:
        """Return dict with stats."""
        return {
            "calls": self._calls,
            "average_time": self._total_time / self._calls
            if self._calls > 0
            else 0,
            "min_time": self._min_time,
            "max_time": self._max_time,
        }


class FunctionStats:
    """Class for tracking stats of calls of functions."""

    def __init__(self) -> None:
        """Init MethodStats."""
        self._stats: defaultdict[str, _FunctionStats] = defaultdict(
            _FunctionStats,
        )

    def __add__(self, other_stats: FunctionStats) -> FunctionStats:
        """Add two instances together."""
        new_stats = FunctionStats()
        for name, stats in self._stats.items():
            new_stats._stats[name] += stats
        for name, stats in other_stats._stats.items():
            new_stats._stats[name] += stats
        return new_stats

    def __iadd__(self, other_stats: FunctionStats) -> FunctionStats:
        """Add instance to self."""
        for name, stats in other_stats._stats.items():
            self._stats[name] += stats
        return self

    def as_dict(self) -> dict[str, dict[str, int | float]]:
        """Return dict with stats."""
        return {name: stats.as_dict() for name, stats in self._stats.items()}

    def get_stats(self, function_name: str) -> dict[str, int | float]:
        """Get stats of a function.

        Args:
            function_name (str)

        Returns:
            dict containing keys "calls", "average_time", "min_time", and
            "max_time".
        """
        return self._stats[function_name].as_dict()

    def wrap(self, function: FuncType) -> FuncType:
        """Decorator that record function execution time.

        Args:
            function (callable): function to time.

        Returns:
            callable with same interface as function.
        """

        def _function(*args: tuple, **kwargs: dict) -> Any:
            start = perf_counter()
            result = function(*args, **kwargs)
            time = perf_counter() - start
            self._stats[function.__name__].add_time(time)
            return result

        return cast(FuncType, _function)

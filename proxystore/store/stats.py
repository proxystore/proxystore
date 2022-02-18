"""Utilities for Tracking Stats on Store Operations."""
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


class KeyedFunctionStats:
    """Class for tracking stats of calls of methods that take a key."""

    def __init__(self) -> None:
        """Init MethodStats."""
        # Nested dict that is keyed on key first then function name
        self._stats: defaultdict[str, dict[str, _FunctionStats]] = defaultdict(
            lambda: defaultdict(_FunctionStats),
        )

    def __add__(self, other_stats: KeyedFunctionStats) -> KeyedFunctionStats:
        """Add two instances together."""
        new_stats = KeyedFunctionStats()
        for key, functions in self._stats.items():
            for function, stats in functions.items():
                new_stats._stats[key][function] += stats
        for key, functions in other_stats._stats.items():
            for function, stats in functions.items():
                new_stats._stats[key][function] += stats
        return new_stats

    def __iadd__(self, other_stats: KeyedFunctionStats) -> KeyedFunctionStats:
        """Add instance to self."""
        for key, functions in other_stats._stats.items():
            for function, stats in functions.items():
                self._stats[key][function] += stats
        return self

    def as_dict(self) -> dict[str, dict[str, dict[str, int | float]]]:
        """Return dict with stats."""
        return {
            key: {
                function: stats.as_dict()
                for function, stats in functions.items()
            }
            for key, functions in self._stats.items()
        }

    def get_stats(self, key: str) -> dict[str, dict[str, int | float]]:
        """Get stats for operations on a key.

        Args:
            key (str)

        Returns:
            dict where keys are function names that have been executed on `key`
            and values are a dict containing stats on the function calls.
        """
        stats = self._stats[key].copy()
        return {f: s.as_dict() for f, s in stats.items()}

    def wrap(self, function: FuncType, key_is_kwarg: bool = False) -> FuncType:
        """Decorator that record function execution time.

        Args:
            function (callable): function to time.
            key_is_kwarg (bool): the key passed to `function` is assumed to be
                the first positional arg. If the key is passed as a kwarg, set
                this to `True` (default: False).

        Returns:
            callable with same interface as function.
        """

        def _function(*args: tuple, **kwargs: dict) -> Any:
            start = perf_counter()
            result = function(*args, **kwargs)
            time = perf_counter() - start
            key = cast(str, kwargs["key"] if key_is_kwarg else args[0])
            self._stats[key][function.__name__].add_time(time)
            return result

        return cast(FuncType, _function)

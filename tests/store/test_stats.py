"""ProxyStore Stat Tests."""
from __future__ import annotations

import time

from proxystore.store.stats import FunctionStats


def list_add(a: int, b: list[int]) -> list[int]:
    """Add a to each element in b."""
    return list(map(lambda x: x + a, b))


def sleep(seconds: float) -> None:
    """Sleep function."""
    time.sleep(seconds)


def test_wrapper_emulates_function() -> None:
    """Test wrapper emulates function."""
    stats = FunctionStats()

    wrapped = stats.wrap(list_add)

    a = 5
    lst = [5, 8, 2, 4, 8]
    assert list_add(a, lst) == wrapped(a, lst)


def test_function_timing() -> None:
    """Test function timing."""
    stats = FunctionStats()

    wrapped = stats.wrap(sleep)

    wrapped(0.01)
    s = stats.as_dict()
    assert "sleep" in s
    s = stats.get_stats("sleep")
    assert s["calls"] == 1
    assert s["average_time"] >= 0.01
    assert s["min_time"] > 0
    assert s["min_time"] == s["max_time"]

    old_max = s["max_time"]
    wrapped(1)
    assert stats.get_stats("sleep")["calls"] == 2
    assert stats.get_stats("sleep")["max_time"] > old_max

    old_min = s["min_time"]
    wrapped(0)
    assert stats.get_stats("sleep")["calls"] == 3
    assert stats.get_stats("sleep")["min_time"] < old_min
    assert 0 < stats.get_stats("sleep")["average_time"] < 1


def test_default_times() -> None:
    """Test default times."""
    stats = FunctionStats()

    s = stats.get_stats("random_function_name")
    assert s["calls"] == 0


def test_merge() -> None:
    """Test merge FunctionStats."""
    stats1 = FunctionStats()
    stats2 = FunctionStats()

    assert stats1.as_dict() == stats2.as_dict() == (stats1 + stats2).as_dict()

    wrapped = stats1.wrap(sleep)
    wrapped(0)

    merged = stats1 + stats2
    assert stats1.as_dict() == merged.as_dict()

    # Should only appear in stats1, not in merged
    wrapped(0)
    assert stats1.get_stats("sleep")["calls"] == 2
    assert merged.get_stats("sleep")["calls"] == 1

    wrapped = stats2.wrap(sleep)
    wrapped(0)

    merged = stats1 + stats2
    assert merged.get_stats("sleep")["calls"] == 3


def test_in_place_add() -> None:
    """Test in place add."""
    stats1 = FunctionStats()
    stats2 = FunctionStats()

    wrapped1 = stats1.wrap(sleep)
    wrapped1(0)

    wrapped2 = stats2.wrap(sleep)
    wrapped2(0)

    stats1 += stats2
    assert stats1.get_stats("sleep")["calls"] == 2

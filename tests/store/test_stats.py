"""ProxyStore Stat Tests."""
from __future__ import annotations

import time

from proxystore.store.stats import KeyedFunctionStats


def list_add(key: str, a: int, b: list[int]) -> list[int]:
    """Add a to each element in b."""
    return list(map(lambda x: x + a, b))


def key_arg(key: str) -> None:
    """Function where key in arg."""
    pass


def key_kwarg(*, key: str) -> None:
    """Function where key is kwarg."""
    pass


def sleep(key: str, seconds: float) -> None:
    """Sleep function."""
    time.sleep(seconds)


def test_wrapper_emulates_function() -> None:
    """Test wrapper emulates function."""
    stats = KeyedFunctionStats()

    wrapped = stats.wrap(list_add)

    a = 5
    lst = [5, 8, 2, 4, 8]
    assert list_add("key", a, lst) == wrapped("key", a, lst)


def test_key_args_vs_kwargs() -> None:
    """Test handling of different key argument positions."""
    stats = KeyedFunctionStats()

    wrapped_arg = stats.wrap(key_arg, key_is_kwarg=False)
    wrapped_arg("key")

    assert stats.get_stats("key")["key_arg"]["calls"] == 1

    wrapped_kwarg = stats.wrap(key_kwarg, key_is_kwarg=True)
    wrapped_kwarg(key="key2")

    assert stats.get_stats("key2")["key_kwarg"]["calls"] == 1


def test_function_timing() -> None:
    """Test function timing."""
    stats = KeyedFunctionStats()

    wrapped = stats.wrap(sleep)

    wrapped("key", 0.01)
    s = stats.as_dict()
    assert "key" in s
    assert "sleep" in s["key"]
    s = stats.get_stats("key")["sleep"]
    assert s["calls"] == 1
    assert s["average_time"] >= 0.01
    assert s["min_time"] > 0
    assert s["min_time"] == s["max_time"]

    old_max = s["max_time"]
    wrapped("key", 1)
    assert stats.get_stats("key")["sleep"]["calls"] == 2
    assert stats.get_stats("key")["sleep"]["max_time"] > old_max

    old_min = s["min_time"]
    wrapped("key", 0)
    assert stats.get_stats("key")["sleep"]["calls"] == 3
    assert stats.get_stats("key")["sleep"]["min_time"] < old_min
    assert 0 < stats.get_stats("key")["sleep"]["average_time"] < 1


def test_default_times() -> None:
    """Test default times."""
    stats = KeyedFunctionStats()

    s = stats.get_stats("random_key")
    assert isinstance(s, dict)
    assert len(s) == 0


def test_merge() -> None:
    """Test merge KeyedFunctionStats."""
    stats1 = KeyedFunctionStats()
    stats2 = KeyedFunctionStats()

    assert stats1.as_dict() == stats2.as_dict() == (stats1 + stats2).as_dict()

    wrapped = stats1.wrap(sleep)
    wrapped("key", 0)

    merged = stats1 + stats2
    assert stats1.as_dict() == merged.as_dict()

    # Should only appear in stats1, not in merged
    wrapped("key", 0)
    assert stats1.get_stats("key")["sleep"]["calls"] == 2
    assert merged.get_stats("key")["sleep"]["calls"] == 1

    wrapped = stats2.wrap(sleep)
    wrapped("key", 0)

    merged = stats1 + stats2
    assert merged.get_stats("key")["sleep"]["calls"] == 3


def test_in_place_add() -> None:
    """Test in place add."""
    stats1 = KeyedFunctionStats()
    stats2 = KeyedFunctionStats()

    wrapped1 = stats1.wrap(sleep)
    wrapped1("key", 0)

    wrapped2 = stats2.wrap(sleep)
    wrapped2("key", 0)

    stats1 += stats2
    assert stats1.get_stats("key")["sleep"]["calls"] == 2

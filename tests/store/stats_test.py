"""ProxyStore Stat Tests."""
from __future__ import annotations

import time
from typing import NamedTuple

from pytest import raises

from proxystore.store.stats import Event
from proxystore.store.stats import FunctionEventStats
from proxystore.store.stats import TimeStats


class _TestKey(NamedTuple):
    """Test key. Store keys are all NamedTuples."""

    key: str


class _TestClass:
    """Test Class."""

    def get_value(self, key: _TestKey, value: int = 0) -> int:
        """Returns value."""
        return value

    def key_arg(self, key: _TestKey) -> None:
        """Function where key in arg."""
        pass

    def key_return(self, *, key: _TestKey) -> _TestKey:
        """Function where key is kwarg."""
        return key


def sleep(key: _TestKey, seconds: float) -> None:
    """Sleep function."""
    time.sleep(seconds)


def test_time_stats() -> None:
    """Test TimeStats dataclass."""
    stats1 = TimeStats(calls=1, avg_time_ms=1, min_time_ms=1, max_time_ms=1)
    stats2 = TimeStats(calls=1, avg_time_ms=3, min_time_ms=3, max_time_ms=3)
    stats3 = stats1 + stats2

    assert stats3.calls == 2
    assert stats3.avg_time_ms == 2
    assert stats3.min_time_ms == 1
    assert stats3.max_time_ms == 3

    stats1.add_time(3)
    assert stats1 == stats3


def test_event_hashes() -> None:
    """Test Event hashes."""
    d = {Event(function='f', key=_TestKey('k')): 1}
    assert d[Event(function='f', key=_TestKey('k'))] == 1


def test_wrapper_emulates_function() -> None:
    """Test wrapper emulates function."""
    stats = FunctionEventStats()
    obj = _TestClass()
    wrapped = stats.wrap(obj.get_value)
    assert obj.get_value(_TestKey('key'), 5) == wrapped(_TestKey('key'), 5)


def test_key_args_vs_return() -> None:
    """Test handling of different key argument positions."""
    stats = FunctionEventStats()
    obj = _TestClass()

    key = _TestKey('key')
    missing_key = _TestKey('missing_key')
    wrapped_arg = stats.wrap(obj.key_arg)
    wrapped_arg(key)
    assert stats[Event(function='key_arg', key=key)].calls == 1

    wrapped_return = stats.wrap(obj.key_return, key_is_result=True)
    wrapped_return(key=key)
    assert stats[Event(function='key_return', key=key)].calls == 1

    # Set key_is_result to True even though it is not to test default key
    # of None.
    wrapped_arg = stats.wrap(obj.key_arg, key_is_result=True)
    wrapped_arg(missing_key)
    assert stats[Event(function='key_arg', key=missing_key)].calls == 0
    assert stats[Event(function='key_arg', key=None)].calls == 1

    # Set key_is_result to False even though it is not to test default key
    # of None
    wrapped_return = stats.wrap(obj.key_return, key_is_result=False)
    wrapped_return(key=missing_key)
    assert stats[Event(function='key_return', key=missing_key)].calls == 0
    assert stats[Event(function='key_return', key=None)].calls == 1


def test_function_timing() -> None:
    """Test function timing."""
    stats = FunctionEventStats()

    key = _TestKey('key')
    wrapped = stats.wrap(sleep)

    event = Event(function='sleep', key=key)
    wrapped(key, 0.01)

    assert event in stats
    assert stats[event].calls == 1
    assert stats[event].avg_time_ms >= 0.001
    assert stats[event].min_time_ms > 0
    assert stats[event].min_time_ms == stats[event].max_time_ms

    old_max = stats[event].max_time_ms
    wrapped(key, 1)
    assert stats[event].calls == 2
    assert stats[event].max_time_ms > old_max

    old_min = stats[event].min_time_ms
    wrapped(key, 0)
    assert stats[event].calls == 3
    assert stats[event].min_time_ms < old_min


def test_default_times() -> None:
    """Test default times."""
    stats = FunctionEventStats()
    assert len(stats) == 0

    event = Event(function='fake', key=_TestKey('fake'))
    assert stats[event].calls == 0


def test_enforces_types() -> None:
    """Test FunctionEventStats enforces types of keys and values."""
    stats = FunctionEventStats()
    key = _TestKey('key')

    with raises(TypeError):
        stats[key]  # type: ignore

    with raises(TypeError):
        stats[Event(function='function', key=key)] = 'value'  # type: ignore

    with raises(TypeError):
        stats[key] = TimeStats()  # type: ignore

    with raises(TypeError):
        stats.update([(key, 'value')])


def test_behaves_like_mapping() -> None:
    """Test FunctionEventStats behaves like a mapping."""
    stats = FunctionEventStats()
    event = Event(function='f', key=_TestKey('k'))

    # __setitem__
    stats[event] = TimeStats(calls=0)
    # __len__
    assert len(stats) == 1
    # __iter__
    for event in stats:
        # __getitem__
        assert stats[event].calls == 0
    # __delitem__
    del stats[event]
    # keys
    assert event not in stats.keys()


def test_update() -> None:
    """Test FunctionEventStats.update()."""
    stats = FunctionEventStats()

    stats.update({Event(function='f', key=_TestKey('k')): TimeStats(calls=1)})
    assert stats[Event(function='f', key=_TestKey('k'))].calls == 1

    stats.update(
        [(Event(function='f', key=_TestKey('k')), TimeStats(calls=2))],
    )
    assert stats[Event(function='f', key=_TestKey('k'))].calls == 3

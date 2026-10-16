from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from proxystore.store.cache import LRUCache


def _set(
    c: LRUCache[str, int],
    key: str,
    value: int,
    size: int = 1,
) -> None:
    future, started = c.start(key)
    assert started
    c.finish(key, future, value, size=size)


def test_lru_raises() -> None:
    with pytest.raises(ValueError, match='Cache size must'):
        LRUCache(-1)


def test_lru_cache() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    # Put 1, 2, 3, 4 in cache
    for i in range(1, 5):
        _set(c, str(i), i)
    for i in range(4, 0, -1):
        assert c.get(str(i)) == i
    # 4 is now least recently used
    _set(c, '5', 5)
    # 4 should now be evicted
    assert c.exists('1')
    assert not c.exists('4')
    assert c.exists('5')
    assert c.get('Fake Key') is None
    assert c.get('Fake Key', 1) == 1
    assert c.hits == 4
    assert c.misses == 2


def test_lru_cache_evict_missing() -> None:
    c: LRUCache[str, int] = LRUCache(1)
    _set(c, '1', 1)
    assert c.exists('1')
    c.evict('1')
    assert not c.exists('1')
    # Should not fail
    c.evict('1')


def test_lru_cache_start_pending() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, started = c.start('1')
    assert started

    # Another thread shares the value while it is filled in, but it is not
    # a cached value yet.
    with ThreadPoolExecutor(1) as pool:
        other, started = pool.submit(c.start, '1').result()
    assert other is future
    assert not started
    assert c.get('1') is None
    assert not c.exists('1')

    c.finish('1', future, 1, size=1)
    assert future.result() == 1
    assert c.get('1') == 1

    # A cached value is returned by start() as a done future.
    cached, started = c.start('1')
    assert not started
    assert cached.result() == 1


def test_lru_cache_start_same_thread() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    outer, _ = c.start('1')

    # The thread filling in the value does not wait on itself.
    inner, started = c.start('1')
    assert started
    assert inner is not outer
    c.finish('1', inner, 2, size=1)
    assert inner.result() == 2
    assert not c.exists('1')

    c.finish('1', outer, 1, size=1)
    assert c.get('1') == 1


def test_lru_cache_finish_without_caching() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, _ = c.start('1')
    c.finish('1', future, 1, size=1, cache=False)
    assert future.result() == 1
    assert not c.exists('1')
    _, started = c.start('1')
    assert started


def test_lru_cache_size_zero() -> None:
    c: LRUCache[str, int] = LRUCache(0)
    future, _ = c.start('1')
    with ThreadPoolExecutor(1) as pool:
        other, started = pool.submit(c.start, '1').result()
    assert other is future
    assert not started

    c.finish('1', future, 1, size=1)
    assert other.result() == 1
    assert not c.exists('1')


def test_lru_cache_size_counts_finished_values() -> None:
    c: LRUCache[str, int] = LRUCache(1)
    pending, _ = c.start('1')
    _set(c, '2', 2)
    _set(c, '3', 3)
    # The pending value is kept and the least recently used value removed.
    assert not c.exists('2')
    assert c.exists('3')

    c.finish('1', pending, 1, size=1)
    assert c.exists('1')
    assert not c.exists('3')


def test_lru_cache_size_bytes() -> None:
    c: LRUCache[str, int] = LRUCache(10)
    _set(c, '1', 1, size=4)
    _set(c, '2', 2, size=4)
    assert c.size_bytes == 8

    # Both values are removed to make room for a larger one.
    _set(c, '3', 3, size=9)
    assert not c.exists('1')
    assert not c.exists('2')
    assert c.size_bytes == 9

    c.evict('3')
    assert c.size_bytes == 0
    c.evict('3')
    assert c.size_bytes == 0


def test_lru_cache_value_larger_than_max() -> None:
    c: LRUCache[str, int] = LRUCache(10)
    _set(c, '1', 1, size=4)
    future, _ = c.start('2')
    c.finish('2', future, 2, size=11)
    assert future.result() == 2

    # The large value is not cached and the cached value is kept.
    assert not c.exists('2')
    assert c.exists('1')
    assert c.size_bytes == 4

    # A value of exactly the max size is cached.
    _set(c, '3', 3, size=10)
    assert c.exists('3')
    assert not c.exists('1')


def test_lru_cache_evict_while_pending() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, _ = c.start('1')
    c.evict('1')

    # A new value is started after the evict.
    with ThreadPoolExecutor(1) as pool:
        new, started = pool.submit(c.start, '1').result()
    assert started
    assert new is not future

    # The old value goes to its waiters but is not cached.
    c.finish('1', future, 1, size=1)
    assert future.result() == 1
    assert c.get('1') is None

    c.finish('1', new, 2, size=1)
    assert c.get('1') == 2


def test_lru_cache_fail() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, _ = c.start('1')
    c.fail('1', future, RuntimeError('failed'))
    with pytest.raises(RuntimeError, match='failed'):
        future.result()

    assert not c.exists('1')
    old, started = c.start('1')
    assert started

    # Failing a value which was evicted does not remove the new value.
    c.evict('1')
    new, _ = c.start('1')
    c.fail('1', old, RuntimeError('failed'))
    with ThreadPoolExecutor(1) as pool:
        other, started = pool.submit(c.start, '1').result()
    assert other is new
    assert not started


def test_lru_cache_after_fork() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    _set(c, '1', 1)
    c.start('2')
    # Another thread holds the lock when the process is forked.
    assert c._lock.acquire(timeout=5)

    # Simulate the fork handler running in a forked child process
    c._reset_after_fork()

    # The value is still cached, but the value being filled in by a thread
    # of the parent is removed.
    assert c.get('1') == 1
    _, started = c.start('2')
    assert started

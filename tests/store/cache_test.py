from __future__ import annotations

from collections.abc import Hashable

import pytest

from proxystore.store.cache import LRUCache


def _set(
    c: LRUCache[str, int],
    key: str,
    value: int,
    variant: Hashable = None,
) -> None:
    future, started = c.start(key, variant)
    assert started
    c.finish(key, variant, future, value)


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
    assert c.get('Fake Key', default=1) == 1
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


def test_lru_cache_variants() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    _set(c, '1', 1, variant='a')
    _set(c, '1', 2, variant='b')
    assert c.get('1', 'a') == 1
    assert c.get('1', 'b') == 2
    assert c.get('1') is None
    assert c.exists('1')

    c.evict('1')
    assert c.get('1', 'a') is None
    assert c.get('1', 'b') is None
    assert not c.exists('1')


def test_lru_cache_start_pending() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, started = c.start('1')
    assert started

    # The entry is shared while it is filled in but is not a cached value.
    other, started = c.start('1')
    assert other is future
    assert not started
    assert c.get('1') is None
    assert not c.exists('1')

    c.finish('1', None, future, 1)
    assert future.result() == 1
    assert c.get('1') == 1

    # A cached value is returned by start() as a done future.
    cached, started = c.start('1')
    assert not started
    assert cached.result() == 1


def test_lru_cache_finish_without_caching() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, _ = c.start('1')
    c.finish('1', None, future, 1, cache=False)
    assert future.result() == 1
    assert not c.exists('1')
    _, started = c.start('1')
    assert started


def test_lru_cache_size_zero() -> None:
    c: LRUCache[str, int] = LRUCache(0)
    future, _ = c.start('1')
    other, started = c.start('1')
    assert other is future
    assert not started

    c.finish('1', None, future, 1)
    assert other.result() == 1
    assert not c.exists('1')


def test_lru_cache_size_counts_finished_values() -> None:
    c: LRUCache[str, int] = LRUCache(1)
    pending, _ = c.start('1')
    _set(c, '2', 2)
    _set(c, '3', 3)
    # The pending entry is kept and the least recently used value removed.
    assert not c.exists('2')
    assert c.exists('3')
    _, started = c.start('1')
    assert not started

    c.finish('1', None, pending, 1)
    assert c.exists('1')
    assert not c.exists('3')


def test_lru_cache_evict_while_pending() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, _ = c.start('1')
    c.evict('1')

    # A new entry is started after the evict.
    new, started = c.start('1')
    assert started
    assert new is not future

    # The old value goes to its waiters but is not cached.
    c.finish('1', None, future, 1)
    assert future.result() == 1
    assert c.get('1') is None

    c.finish('1', None, new, 2)
    assert c.get('1') == 2


def test_lru_cache_fail() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    future, _ = c.start('1')
    c.fail('1', None, future, RuntimeError('failed'))
    with pytest.raises(RuntimeError, match='failed'):
        future.result()

    assert not c.exists('1')
    old, started = c.start('1')
    assert started

    # Failing an entry which was evicted does not remove the new entry.
    c.evict('1')
    new, _ = c.start('1')
    c.fail('1', None, old, RuntimeError('failed'))
    other, started = c.start('1')
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

    # The value is still cached, but the entry being filled in by a thread
    # of the parent is removed.
    assert c.get('1') == 1
    _, started = c.start('2')
    assert started

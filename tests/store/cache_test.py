from __future__ import annotations

import pytest

from proxystore.store.cache import LRUCache


def test_lru_raises() -> None:
    with pytest.raises(ValueError, match='Cache size must'):
        LRUCache(-1)


def test_lru_cache() -> None:
    c: LRUCache[str, int] = LRUCache(4)
    # Put 1, 2, 3, 4 in cache
    for i in range(1, 5):
        c.set(str(i), i)
    for i in range(4, 0, -1):
        assert c.get(str(i)) == i
    # 4 is now least recently used
    c.set('5', 5)
    # 4 should now be evicted
    assert c.exists('1')
    assert not c.exists('4')
    assert c.exists('5')
    assert c.get('Fake Key', None) is None
    assert c.get('Fake Key', 1) == 1


def test_lru_cache_evict_missing() -> None:
    c: LRUCache[str, int] = LRUCache(1)
    c.set('1', 1)
    assert c.exists('1')
    c.evict('1')
    assert not c.exists('1')
    # Should not fail
    c.evict('1')


def test_lru_cache_set_existing_key() -> None:
    c: LRUCache[str, int] = LRUCache(2)
    c.set('1', 1)
    c.set('1', 2)
    assert c.get('1') == 2
    c.evict('1')
    assert not c.exists('1')
    # A stale LRU entry for '1' would cause a KeyError once the cache is full
    for key in ('2', '3', '4'):
        c.set(key, 0)
    assert not c.exists('2')
    assert c.exists('3')
    assert c.exists('4')


def test_lru_cache_set_existing_key_updates_order() -> None:
    c: LRUCache[str, int] = LRUCache(2)
    c.set('1', 1)
    c.set('2', 2)
    c.set('1', 3)
    # '2' is now least recently used
    c.set('3', 3)
    assert c.get('1') == 3
    assert not c.exists('2')
    assert c.exists('3')

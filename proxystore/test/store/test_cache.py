"""Cache Unit Tests."""
import multiprocessing as mp

from pytest import raises

from proxystore.store.cache import LRUCache


def test_lru_raises() -> None:
    """Test LRU Error Handling."""
    with raises(ValueError):
        LRUCache(-1)


def test_lru_cache() -> None:
    """Test LRU Cache."""
    c = LRUCache(4)
    # Put 1, 2, 3, 4 in cache
    for i in range(1, 5):
        c.set(str(i), i)
    for i in range(4, 0, -1):
        assert c.get(str(i)) == i
    # 4 is now least recently used
    c.set("5", 5)
    # 4 should now be evicted
    assert c.exists("1")
    assert not c.exists("4")
    assert c.exists("5")
    assert c.get("Fake Key", None) is None
    assert c.get("Fake Key", 1) == 1

    c = LRUCache(1)
    c.set("1", 1)
    assert c.exists("1")
    c.evict("1")
    assert not c.exists("1")
    c.evict("1")


def test_lru_cache_mp() -> None:
    """Test LRU Cache with Multiprocessing."""
    return
    c = LRUCache(1)
    c.set("test_key", "test_value")

    def f(x):
        _c = LRUCache(1)
        assert _c.hits == 0
        assert _c.misses == 0
        assert _c.get("test_key") == "test_value"
        assert _c.hits == 1
        assert _c.misses == 0
        assert _c.get(x) is None
        assert _c.hits == 1
        assert _c.misses == 1

    with mp.Pool(2) as p:
        p.map(f, ["1", "2", "3"])

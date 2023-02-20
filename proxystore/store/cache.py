"""Simple Cache Implementation."""
from __future__ import annotations

from typing import Generic
from typing import TypeVar

KeyT = TypeVar('KeyT')
ValueT = TypeVar('ValueT')


class LRUCache(Generic[KeyT, ValueT]):
    """Simple LRU Cache.

    Args:
        maxsize: Maximum number of value to cache.

    Raises:
        ValueError: If `maxsize <= 0`.
    """

    def __init__(self, maxsize: int = 16) -> None:
        if maxsize < 0:
            raise ValueError('Cache size must by >= 0')
        self.maxsize = maxsize
        self.data: dict[KeyT, ValueT] = {}
        self.lru: list[KeyT] = []

        # Count hits/misses
        self.hits = 0
        self.misses = 0

    def evict(self, key: KeyT) -> None:
        """Evict key from cache."""
        if key in self.data:
            del self.data[key]
            self.lru.remove(key)

    def exists(self, key: KeyT) -> bool:
        """Check if key is in cache."""
        return key in self.data

    def get(self, key: KeyT, default: ValueT | None = None) -> ValueT | None:
        """Get value for key if it exists else returns default."""
        if self.exists(key):
            # Move to front b/c most recently used
            self.hits += 1
            self.lru.remove(key)
            self.lru.insert(0, key)
            return self.data[key]
        else:
            self.misses += 1
            return default

    def set(self, key: KeyT, value: ValueT) -> None:
        """Set key to value."""
        if self.maxsize == 0:
            return
        if len(self.data) >= self.maxsize:
            lru_key = self.lru.pop()
            del self.data[lru_key]
        self.lru.insert(0, key)
        self.data[key] = value

"""Simple Cache Implementation."""

from __future__ import annotations

import threading
from typing import cast
from typing import Generic
from typing import TypeVar

KeyT = TypeVar('KeyT')
ValueT = TypeVar('ValueT')
_MISSING_OBJECT = object()


class LRUCache(Generic[KeyT, ValueT]):
    """Simple thread-safe LRU Cache.

    Args:
        maxsize: Maximum number of value to cache.

    Raises:
        ValueError: If `maxsize <= 0`.
    """

    def __init__(self, maxsize: int = 16) -> None:
        if maxsize < 0:
            raise ValueError('Cache size must by >= 0')
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0

        self._data: dict[KeyT, ValueT] = {}
        self._lru: list[KeyT] = []
        self._lock = threading.RLock()

    def evict(self, key: KeyT) -> None:
        """Evict key from cache."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._lru.remove(key)

    def exists(self, key: KeyT) -> bool:
        """Check if key is in cache."""
        with self._lock:
            return key in self._data

    def get(self, key: KeyT, default: ValueT | None = None) -> ValueT | None:
        """Get value for key if it exists else returns default."""
        with self._lock:
            value = self._data.get(key, _MISSING_OBJECT)
            if value is not _MISSING_OBJECT:
                self._lru.remove(key)
                self._lru.insert(0, key)
                self.hits += 1
                return cast(ValueT, value)

        self.misses += 1
        return default

    def set(self, key: KeyT, value: ValueT) -> None:
        """Set key to value."""
        if self.maxsize == 0:
            return

        with self._lock:
            if len(self._data) >= self.maxsize:
                lru_key = self._lru.pop()
                del self._data[lru_key]
            self._lru.insert(0, key)
            self._data[key] = value

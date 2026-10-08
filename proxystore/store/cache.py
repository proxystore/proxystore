"""Cache of values which are filled in once.

Warning:
    This module is an internal implementation detail which may change
    between releases without notice (see
    [Versioning and Compatibility](../../versioning.md)).
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Hashable
from concurrent.futures import Future
from typing import Generic
from typing import TypeVar

KeyT = TypeVar('KeyT')
ValueT = TypeVar('ValueT')


class LRUCache(Generic[KeyT, ValueT]):
    """Thread-safe LRU cache of values which are filled in once.

    Values are cached by a key and a variant. The
    [`Store`][proxystore.store.base.Store] uses the key of an object in the
    connector and the deserializer used to make the object, so the same key
    read with a different deserializer is a different value.

    An entry is filled in by one thread. The first thread to call
    [`start()`][proxystore.store.cache.LRUCache.start] for an entry fills
    it in and then calls
    [`finish()`][proxystore.store.cache.LRUCache.finish] or
    [`fail()`][proxystore.store.cache.LRUCache.fail]. Other threads which
    call [`start()`][proxystore.store.cache.LRUCache.start] for the entry
    in the meantime get the same future to wait on.

    Args:
        maxsize: Maximum number of finished values to cache. If 0, values
            are not cached once finished, but threads still share a value
            while it is being filled in.

    Raises:
        ValueError: If `maxsize < 0`.
    """

    def __init__(self, maxsize: int = 16) -> None:
        if maxsize < 0:
            raise ValueError('Cache size must be >= 0')
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0

        # Least recently used first. The futures of entries being filled in
        # are not done. Failed entries are removed, so a done future always
        # has a value.
        self._entries: OrderedDict[tuple[KeyT, Hashable], Future[ValueT]] = (
            OrderedDict()
        )
        self._lock = threading.Lock()

    def _reset_after_fork(self) -> None:
        # The lock may have been held by another thread of the parent when
        # the process was forked, and the threads filling in entries do not
        # exist in the child so those entries would never finish.
        self._lock = threading.Lock()
        self._entries = OrderedDict(
            (k, f) for k, f in self._entries.items() if f.done()
        )

    def evict(self, key: KeyT) -> None:
        """Remove every entry of a key.

        Entries being filled in are also removed, so their values are not
        cached when finished. Threads waiting on them still get the value.
        """
        with self._lock:
            for k in [k for k in self._entries if k[0] == key]:
                del self._entries[k]

    def exists(self, key: KeyT) -> bool:
        """Check if a value of a key is cached with any variant."""
        with self._lock:
            return any(
                k[0] == key and f.done() for k, f in self._entries.items()
            )

    def get(
        self,
        key: KeyT,
        variant: Hashable = None,
        default: ValueT | None = None,
    ) -> ValueT | None:
        """Get the cached value of a key and variant.

        Returns:
            The value or `default` if the value is not cached or is still \
            being filled in.
        """
        k = (key, variant)
        with self._lock:
            future = self._entries.get(k)
            if future is not None and future.done():
                self._entries.move_to_end(k)
                self.hits += 1
                return future.result()
            self.misses += 1
        return default

    def start(
        self,
        key: KeyT,
        variant: Hashable = None,
    ) -> tuple[Future[ValueT], bool]:
        """Get the entry of a key and variant or start filling it in.

        Returns:
            The future of the entry and `True` if the caller must fill in \
            the entry by calling \
            [`finish()`][proxystore.store.cache.LRUCache.finish] or \
            [`fail()`][proxystore.store.cache.LRUCache.fail] with the \
            future. If `False`, the value is cached or another thread is \
            filling it in.
        """
        k = (key, variant)
        with self._lock:
            future = self._entries.get(k)
            if future is not None:
                return future, False
            future = Future()
            self._entries[k] = future
            return future, True

    def finish(
        self,
        key: KeyT,
        variant: Hashable,
        future: Future[ValueT],
        value: ValueT,
        *,
        cache: bool = True,
    ) -> None:
        """Finish filling in an entry with a value.

        Threads waiting on the entry get the value. The value is cached if
        `cache` is `True` and the entry was not
        [evicted][proxystore.store.cache.LRUCache.evict] while it was
        being filled in.
        """
        k = (key, variant)
        with self._lock:
            future.set_result(value)
            if self._entries.get(k) is not future:
                return
            if not cache or self.maxsize == 0:
                del self._entries[k]
                return
            self._entries.move_to_end(k)
            done = [k for k, f in self._entries.items() if f.done()]
            for k in done[: max(0, len(done) - self.maxsize)]:
                del self._entries[k]

    def fail(
        self,
        key: KeyT,
        variant: Hashable,
        future: Future[ValueT],
        exception: BaseException,
    ) -> None:
        """Fail filling in an entry.

        Threads waiting on the entry get the exception, and the entry is
        removed so the next [`start()`][proxystore.store.cache.LRUCache.start]
        fills it in again.
        """
        k = (key, variant)
        with self._lock:
            future.set_exception(exception)
            if self._entries.get(k) is future:
                del self._entries[k]

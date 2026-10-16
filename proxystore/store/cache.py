"""Cache of values which are filled in once.

Warning:
    This module is an internal implementation detail which may change
    between releases without notice (see
    [Versioning and Compatibility](../../versioning.md)).
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from concurrent.futures import Future
from typing import Generic
from typing import TypeVar

KeyT = TypeVar('KeyT')
ValueT = TypeVar('ValueT')

DEFAULT_CACHE_SIZE_BYTES = 256 * 1024 * 1024
"""Default maximum size of a cache in bytes (256 MiB)."""


class LRUCache(Generic[KeyT, ValueT]):
    """Thread-safe LRU cache of values which are filled in once.

    The value of a key is filled in by one thread. The first thread to
    call [`start()`][proxystore.store.cache.LRUCache.start] for a key fills
    in the value and then calls
    [`finish()`][proxystore.store.cache.LRUCache.finish] or
    [`fail()`][proxystore.store.cache.LRUCache.fail]. Other threads which
    call [`start()`][proxystore.store.cache.LRUCache.start] for the key in
    the meantime get the same future to wait on.

    The size of each value is given to
    [`finish()`][proxystore.store.cache.LRUCache.finish], and the least
    recently used values are removed while the total size is more than
    `max_bytes`.

    Args:
        max_bytes: Maximum total size of the cached values in bytes. A value
            larger than this is not cached. If 0, values are not cached once
            finished, but threads still share a value while it is being
            filled in.

    Raises:
        ValueError: If `max_bytes < 0`.
    """

    def __init__(self, max_bytes: int = DEFAULT_CACHE_SIZE_BYTES) -> None:
        if max_bytes < 0:
            raise ValueError('Cache size must be >= 0')
        self.max_bytes = max_bytes
        self.size_bytes = 0
        self.hits = 0
        self.misses = 0

        # Values and their sizes, least recently used first.
        self._values: OrderedDict[KeyT, tuple[ValueT, int]] = OrderedDict()
        # Futures of values being filled in and the ID of the thread
        # filling in each one.
        self._pending: dict[KeyT, tuple[Future[ValueT], int]] = {}
        self._lock = threading.Lock()

    def _reset_after_fork(self) -> None:
        # The lock may have been held by another thread of the parent when
        # the process was forked, and the threads filling in values do not
        # exist in the child so those values would never be finished. The
        # futures are not used here because their locks may also have been
        # held.
        self._lock = threading.Lock()
        self._pending = {}

    def evict(self, key: KeyT) -> None:
        """Remove the value of a key.

        A value being filled in is also removed, so it is not cached when
        finished. Threads waiting on it still get the value.
        """
        with self._lock:
            self._remove(key)
            self._pending.pop(key, None)

    def _remove(self, key: KeyT) -> None:
        # Remove a cached value. The caller must hold the lock.
        item = self._values.pop(key, None)
        if item is not None:
            self.size_bytes -= item[1]

    def exists(self, key: KeyT) -> bool:
        """Check if the value of a key is cached."""
        with self._lock:
            return key in self._values

    def get(self, key: KeyT, default: ValueT | None = None) -> ValueT | None:
        """Get the cached value of a key.

        Returns:
            The value or `default` if the value is not cached or is still \
            being filled in.
        """
        with self._lock:
            item = self._values.get(key)
            if item is not None:
                self._values.move_to_end(key)
                self.hits += 1
                return item[0]
            self.misses += 1
        return default

    def start(self, key: KeyT) -> tuple[Future[ValueT], bool]:
        """Get the value of a key or start filling it in.

        If this thread is already filling in the value of the key (e.g.,
        the code filling in the value calls this again), a new future is
        returned for this thread to fill in so it does not wait on itself.
        That value is not cached.

        Returns:
            A future of the value and `True` if the caller must fill in \
            the value by calling \
            [`finish()`][proxystore.store.cache.LRUCache.finish] or \
            [`fail()`][proxystore.store.cache.LRUCache.fail] with the \
            future. If `False`, the value is cached or another thread is \
            filling it in.
        """
        future: Future[ValueT] = Future()
        thread = threading.get_ident()
        with self._lock:
            item = self._values.get(key)
            if item is not None:
                self._values.move_to_end(key)
                future.set_result(item[0])
                return future, False

            pending = self._pending.get(key)
            if pending is None:
                self._pending[key] = (future, thread)
                return future, True
            if pending[1] == thread:
                return future, True
            return pending[0], False

    def finish(
        self,
        key: KeyT,
        future: Future[ValueT],
        value: ValueT,
        *,
        size: int,
        cache: bool = True,
    ) -> None:
        """Finish filling in a value.

        Threads waiting on the value get it. The value is cached if `cache`
        is `True`, `size` is at most `max_bytes`, and the value was not
        [evicted][proxystore.store.cache.LRUCache.evict] while it was being
        filled in.

        Args:
            key: Key of the value.
            future: Future returned by
                [`start()`][proxystore.store.cache.LRUCache.start].
            value: Value of the key.
            size: Size of the value in bytes.
            cache: Cache the value.
        """
        with self._lock:
            pending = self._pending.get(key)
            if pending is not None and pending[0] is future:
                del self._pending[key]
                if cache and self.max_bytes > 0 and size <= self.max_bytes:
                    self._remove(key)
                    self._values[key] = (value, size)
                    self.size_bytes += size
                    while self.size_bytes > self.max_bytes:
                        _, (_, removed) = self._values.popitem(last=False)
                        self.size_bytes -= removed
        future.set_result(value)

    def fail(
        self,
        key: KeyT,
        future: Future[ValueT],
        exception: BaseException,
    ) -> None:
        """Fail filling in a value.

        Threads waiting on the value get the exception, and the next
        [`start()`][proxystore.store.cache.LRUCache.start] fills in the
        value again.
        """
        with self._lock:
            pending = self._pending.get(key)
            if pending is not None and pending[0] is future:
                del self._pending[key]
        future.set_exception(exception)

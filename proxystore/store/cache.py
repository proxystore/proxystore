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
from typing import cast
from typing import Generic
from typing import TypeVar

KeyT = TypeVar('KeyT')
ValueT = TypeVar('ValueT')
_MISSING_OBJECT = object()


class LRUCache(Generic[KeyT, ValueT]):
    """Thread-safe LRU cache of values which are filled in once.

    The value of a key is filled in by one thread. The first thread to
    call [`start()`][proxystore.store.cache.LRUCache.start] for a key fills
    in the value and then calls
    [`finish()`][proxystore.store.cache.LRUCache.finish] or
    [`fail()`][proxystore.store.cache.LRUCache.fail]. Other threads which
    call [`start()`][proxystore.store.cache.LRUCache.start] for the key in
    the meantime get the same future to wait on.

    Args:
        maxsize: Maximum number of values to cache. If 0, values are not
            cached once finished, but threads still share a value while it
            is being filled in.

    Raises:
        ValueError: If `maxsize < 0`.
    """

    def __init__(self, maxsize: int = 16) -> None:
        if maxsize < 0:
            raise ValueError('Cache size must be >= 0')
        self.maxsize = maxsize
        self.hits = 0
        self.misses = 0

        # Values, least recently used first.
        self._values: OrderedDict[KeyT, ValueT] = OrderedDict()
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
            self._values.pop(key, None)
            self._pending.pop(key, None)

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
            value = self._values.get(key, _MISSING_OBJECT)
            if value is not _MISSING_OBJECT:
                self._values.move_to_end(key)
                self.hits += 1
                return cast(ValueT, value)
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
            value = self._values.get(key, _MISSING_OBJECT)
            if value is not _MISSING_OBJECT:
                self._values.move_to_end(key)
                future.set_result(cast(ValueT, value))
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
        cache: bool = True,
    ) -> None:
        """Finish filling in a value.

        Threads waiting on the value get it. The value is cached if `cache`
        is `True` and the value was not
        [evicted][proxystore.store.cache.LRUCache.evict] while it was being
        filled in.
        """
        with self._lock:
            pending = self._pending.get(key)
            if pending is not None and pending[0] is future:
                del self._pending[key]
                if cache and self.maxsize > 0:
                    self._values[key] = value
                    self._values.move_to_end(key)
                    if len(self._values) > self.maxsize:
                        self._values.popitem(last=False)
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

"""Factory implementations."""

from __future__ import annotations

import copy
import logging
import os
import threading
import time
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any
from typing import cast
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeVar

from proxystore._compat import drop_unknown_fields
from proxystore._compat import STATE_VERSION_KEY
from proxystore.store.config import StoreConfig
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.future import PollingPolicy
from proxystore.store.registry import registry
from proxystore.store.types import ConnectorKeyT
from proxystore.store.types import ConnectorT
from proxystore.utils.timer import Timer

if TYPE_CHECKING:
    from proxystore.store.base import Store

logger = logging.getLogger(__name__)

_STATE_VERSION = 1
_MISSING_OBJECT = object()

T = TypeVar('T')

# Thread pool used by StoreFactory.resolve_async(). The pool is created when
# first used and is discarded in a forked child process. The child inherits
# the pool of the parent but not its threads, so the pool would never run
# the tasks submitted by the child.
_default_pool: ThreadPoolExecutor | None = None
_default_pool_lock = threading.Lock()


def _get_default_pool() -> ThreadPoolExecutor:
    global _default_pool  # noqa: PLW0603
    with _default_pool_lock:
        if _default_pool is None:
            _default_pool = ThreadPoolExecutor()
        return _default_pool


def _reset_default_pool_after_fork() -> None:
    global _default_pool, _default_pool_lock  # noqa: PLW0603
    # The lock may have been held by another thread of the parent when the
    # process was forked.
    _default_pool_lock = threading.Lock()
    _default_pool = None


if hasattr(os, 'register_at_fork'):  # pragma: no branch
    os.register_at_fork(after_in_child=_reset_default_pool_after_fork)

# Guards the resolve in progress of every factory. It is only held to read
# or change that state, never while resolving. Reentrant in case the garbage
# collector runs code which calls a factory while this thread holds it.
_resolve_lock = threading.RLock()
# Incremented in a forked child so resolves started before the fork, which
# never finish in the child, are ignored. Cheaper to check than os.getpid().
_fork_generation = 0


def _reset_resolve_lock_after_fork() -> None:
    global _resolve_lock, _fork_generation  # noqa: PLW0603
    # The lock may have been held by another thread of the parent.
    _resolve_lock = threading.RLock()
    _fork_generation += 1


if hasattr(os, 'register_at_fork'):  # pragma: no branch
    os.register_at_fork(after_in_child=_reset_resolve_lock_after_fork)


class _Resolve:
    """A resolve of a factory in progress, which other calls wait on."""

    __slots__ = (
        'done',
        'error',
        'event',
        'future',
        'generation',
        'thread',
        'value',
    )

    def __init__(self, thread: int | None) -> None:
        # Thread resolving the object, so a call from that thread while it
        # resolves (e.g., by a deserializer) resolves again rather than
        # waiting on itself. Set by the pool thread for resolve_async().
        self.thread = thread
        # Fork generation of the process which started the resolve. A
        # forked child gets a copy which never finishes, so it is ignored.
        self.generation = _fork_generation
        self.done = False
        self.value: Any = None
        self.error: BaseException | None = None
        # Made by the first call which waits, so a call without other
        # threads does not pay for it.
        self.event: Event | None = None
        # Set by resolve_async(). The result is kept until a call takes it.
        self.future: Future[Any] | None = None


class StoreFactory(Generic[ConnectorT, T]):
    """Factory that resolves an object from a store.

    Adds support for asynchronously retrieving objects from a
    [`Store`][proxystore.store.base.Store] instance.

    The factory takes the `store_config` parameter that is
    used to reinitialize the store if the factory is sent to a remote
    process where the store has not already been initialized.

    Calls to the factory from threads while another call (or
    [`resolve_async()`][proxystore.store.factory.StoreFactory.resolve_async])
    is resolving the object wait for that result, so the object is resolved
    once and every thread gets the same object.

    Args:
        key: Key corresponding to object in store.
        store_config: Store configuration used to reinitialize the store if
            needed.
        evict: If True, evict the object from the store once
            [`resolve()`][proxystore.store.factory.StoreFactory.resolve]
            is called.
    """

    def __init__(
        self,
        key: ConnectorKeyT,
        store_config: StoreConfig,
        *,
        evict: bool = False,
    ) -> None:
        # The key is typed Any because the type of the store's connector,
        # and so the type of its keys, is not known from a proxy.
        self.key: Any = key
        self.store_config = store_config
        self.evict = evict

        # Not included when a factory is serialized because it is specific
        # to this instance of the factory.
        self._resolve: _Resolve | None = None

    def __call__(self) -> T:
        with Timer() as timer:
            obj = self._resolve_once()

        store = self.get_store()
        if store.metrics is not None:
            store.metrics.add_time('factory.call', self.key, timer.elapsed_ms)

        return obj

    # The pickled state of factories is part of the format of proxies which
    # must be compatible between 2.x versions. Fields can be added (with
    # defaults in __setstate__) but not removed or renamed. See
    # proxystore._compat for details.
    _STATE_FIELDS: frozenset[str] = frozenset(
        (STATE_VERSION_KEY, 'key', 'store_config', 'evict'),
    )

    def __getstate__(self) -> dict[str, Any]:
        # A possible future is not included because it is specific to this
        # instance of the factory.
        return {
            STATE_VERSION_KEY: _STATE_VERSION,
            'key': self.key,
            'store_config': self.store_config,
            'evict': self.evict,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        state = drop_unknown_fields(
            type(self).__name__,
            state,
            self._STATE_FIELDS,
        )
        self.key = state['key']
        self.store_config = state['store_config']
        self.evict = state.get('evict', False)
        self._resolve = None

    def get_store(self) -> Store[ConnectorT]:
        """Get store and reinitialize if necessary.

        Raises:
            StoreClosedError: If the owner of the store closed it and
                removed its objects in this process.
        """
        return registry.get_or_create(self.store_config)

    def resolve(self) -> T:
        """Get object associated with key from store.

        Raises:
            ProxyResolveMissingKeyError: If the key associated with this
                factory does not exist in the store.
            StoreClosedError: If the owner of the store closed it and
                removed its objects in this process.
        """
        with Timer() as timer:
            store = self.get_store()
            obj = store.get(self.key, default=_MISSING_OBJECT)

            if obj is _MISSING_OBJECT:
                raise ProxyResolveMissingKeyError(
                    self.key,
                    type(store),
                    store.name,
                    store.id,
                )

            if self.evict:
                store.evict(self.key)

        if store.metrics is not None:
            total_time = timer.elapsed_ms
            store.metrics.add_time('factory.resolve', self.key, total_time)

        return cast(T, obj)

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from store.

        Nothing happens if the object is already being resolved.
        """
        with _resolve_lock:
            current = self._resolve
            if current is not None and current.generation == _fork_generation:
                return
            logger.debug('Starting asynchronous resolve of %s', self.key)
            resolve = _Resolve(thread=None)
            resolve.future = _get_default_pool().submit(
                self._resolve_in_pool,
                resolve,
            )
            self._resolve = resolve

    def _resolve_in_pool(self, resolve: _Resolve) -> T:
        resolve.thread = threading.get_ident()
        return self.resolve()

    def _resolve_once(self) -> T:
        # Calls from other threads while the object is being resolved wait
        # for that result, so the object is resolved once and every thread
        # gets the same object.
        thread = threading.get_ident()
        with _resolve_lock:
            current = self._resolve
            if current is None or current.generation != _fork_generation:
                current = _Resolve(thread)
                self._resolve = current
                role = 'owner'
            elif current.thread == thread:
                role = 'reentrant'
            else:
                role = 'waiter'
                # A finished resolve is never seen here because it is
                # removed when it finishes.
                if current.future is None and current.event is None:
                    current.event = Event()

        if role == 'reentrant':
            # Called while this thread resolves the object.
            return self.resolve()
        if role == 'waiter':
            return self._wait(current)

        try:
            obj = self.resolve()
        except BaseException as e:
            self._finish(current, error=e)
            raise
        self._finish(current, value=obj)
        return obj

    def _finish(
        self,
        resolve: _Resolve,
        *,
        value: Any = None,
        error: BaseException | None = None,
    ) -> None:
        with _resolve_lock:
            resolve.value = value
            resolve.error = error
            resolve.done = True
            if self._resolve is resolve:
                self._resolve = None
            event = resolve.event
        if event is not None:
            event.set()

    def _wait(self, resolve: _Resolve) -> T:
        if resolve.future is not None:
            try:
                return resolve.future.result()
            finally:
                # Taking the result of resolve_async() means later calls
                # resolve again, as they did before it was used.
                with _resolve_lock:
                    if self._resolve is resolve:
                        self._resolve = None
        assert resolve.event is not None
        resolve.event.wait()
        if resolve.error is not None:
            raise resolve.error
        return cast(T, resolve.value)


class PollingStoreFactory(StoreFactory[ConnectorT, T]):
    """Factory that polls a store until an object can be resolved.

    This is an extension of the
    [`StoreFactory`][proxystore.store.factory.StoreFactory] with the
    [`resolve()`][proxystore.store.factory.StoreFactory.resolve] method
    overridden to poll the store until the target object is available.
    If the object is an exception, it is raised instead of returned.

    Args:
        key: Key corresponding to object in store.
        store_config: Store configuration used to reinitialize the store if
            needed.
        evict: If True, evict the object from the store once
            [`resolve()`][proxystore.store.factory.StoreFactory.resolve]
            is called.
        polling: Policy for polling the store for the object. If `None`,
            the default
            [`PollingPolicy`][proxystore.store.future.PollingPolicy] is used.
    """

    def __init__(
        self,
        key: ConnectorKeyT,
        store_config: StoreConfig,
        *,
        evict: bool = False,
        polling: PollingPolicy | None = None,
    ) -> None:
        super().__init__(key, store_config, evict=evict)
        self.polling = polling if polling is not None else PollingPolicy()

    # The polling policy is pickled as separate fields rather than as a
    # PollingPolicy so the pickle format does not depend on that type.
    _STATE_FIELDS = StoreFactory._STATE_FIELDS | {
        'polling_interval',
        'polling_backoff_factor',
        'polling_interval_limit',
        'polling_timeout',
    }

    def __getstate__(self) -> dict[str, Any]:
        state = super().__getstate__()
        state['polling_interval'] = self.polling.interval
        state['polling_backoff_factor'] = self.polling.backoff_factor
        state['polling_interval_limit'] = self.polling.interval_limit
        state['polling_timeout'] = self.polling.timeout
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        super().__setstate__(state)
        self.polling = PollingPolicy(
            interval=state.get('polling_interval', 1),
            backoff_factor=state.get('polling_backoff_factor', 1),
            interval_limit=state.get('polling_interval_limit'),
            timeout=state.get('polling_timeout'),
        )

    def _poll(self, timeout: float | None) -> tuple[T] | None:
        # Poll the store for the object until timeout seconds have elapsed.
        # Returns the object in a tuple, to distinguish an object which is
        # None, or None if the timeout was reached. Raises the object if it
        # is an exception.
        with Timer() as timer:
            store = self.get_store()
            sleep_interval = self.polling.interval
            # The deadline uses the clock, not the sum of the sleeps, so
            # time spent in store.get() counts toward the timeout.
            deadline = None if timeout is None else time.monotonic() + timeout

            while True:
                obj = store.get(self.key, default=_MISSING_OBJECT)
                if obj is not _MISSING_OBJECT:
                    break

                if deadline is None:
                    time.sleep(sleep_interval)
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    # Do not sleep past the deadline, but still check the
                    # store one last time when it is reached.
                    time.sleep(min(sleep_interval, remaining))

                new_interval = sleep_interval * self.polling.backoff_factor
                sleep_interval = (
                    new_interval
                    if self.polling.interval_limit is None
                    else min(new_interval, self.polling.interval_limit)
                )

            if obj is _MISSING_OBJECT:
                return None
            if isinstance(obj, BaseException):
                # The store may cache the exception object and return it on
                # every get. Raise a copy so the cached object does not
                # collect a traceback (and the frames it holds) each time.
                raise copy.copy(obj)
            if self.evict:
                store.evict(self.key)

        if store.metrics is not None:
            total_time = timer.elapsed_ms
            store.metrics.add_time(
                'factory.polling_resolve',
                self.key,
                total_time,
            )

        return (cast(T, obj),)

    def resolve(self) -> T:
        """Get object associated with key from store.

        Raises:
            ProxyResolveMissingKeyError: If the object associated with the
                key is not available after the timeout of the polling policy.
            Exception: The exception set on the future associated with this
                factory.
        """
        obj = self._poll(self.polling.timeout)
        if obj is None:
            store = self.get_store()
            raise ProxyResolveMissingKeyError(
                self.key,
                type(store),
                store.name,
                store.id,
            )
        return obj[0]

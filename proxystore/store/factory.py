"""Factory implementations."""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import cast
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeVar

from proxystore._compat import drop_unknown_fields
from proxystore._compat import STATE_VERSION_KEY
from proxystore.store.config import StoreConfig
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.future import _deserialize_with_exceptions
from proxystore.store.future import _FutureException
from proxystore.store.future import PollingPolicy
from proxystore.store.registry import registry
from proxystore.store.types import ConnectorKeyT
from proxystore.store.types import ConnectorT
from proxystore.store.types import DeserializerT
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


class StoreFactory(Generic[ConnectorT, T]):
    """Factory that resolves an object from a store.

    Adds support for asynchronously retrieving objects from a
    [`Store`][proxystore.store.base.Store] instance.

    The factory takes the `store_config` parameter that is
    used to reinitialize the store if the factory is sent to a remote
    process where the store has not already been initialized.

    Args:
        key: Key corresponding to object in store.
        store_config: Store configuration used to reinitialize the store if
            needed.
        evict: If True, evict the object from the store once
            [`resolve()`][proxystore.store.factory.StoreFactory.resolve]
            is called.
        deserializer: Optional callable used to deserialize the byte string.
            If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be used.
    """

    def __init__(
        self,
        key: ConnectorKeyT,
        store_config: StoreConfig,
        *,
        evict: bool = False,
        deserializer: DeserializerT | None = None,
    ) -> None:
        self.key = key
        self.store_config = store_config
        self.evict = evict
        self.deserializer = deserializer

        # The following are not included when a factory is serialized
        # because they are specific to that instance of the factory
        self._obj_future: Future[T] | None = None

    def __call__(self) -> T:
        with Timer() as timer:
            if self._obj_future is not None:
                obj = self._obj_future.result()
                self._obj_future = None
            else:
                obj = self.resolve()

        store = self.get_store()
        if store.metrics is not None:
            store.metrics.add_time('factory.call', self.key, timer.elapsed_ms)

        return obj

    # The pickled state of factories is part of the format of proxies which
    # must be compatible between 2.x versions. Fields can be added (with
    # defaults in __setstate__) but not removed or renamed. See
    # proxystore._compat for details.
    _STATE_FIELDS: frozenset[str] = frozenset(
        (STATE_VERSION_KEY, 'key', 'store_config', 'evict', 'deserializer'),
    )

    def __getstate__(self) -> dict[str, Any]:
        # A possible future is not included because it is specific to this
        # instance of the factory.
        return {
            STATE_VERSION_KEY: _STATE_VERSION,
            'key': self.key,
            'store_config': self.store_config,
            'evict': self.evict,
            'deserializer': self.deserializer,
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
        self.deserializer = state.get('deserializer')
        self._obj_future = None

    def get_store(self) -> Store[ConnectorT]:
        """Get store and reinitialize if necessary."""
        return registry.get_or_create(self.store_config)

    def resolve(self) -> T:
        """Get object associated with key from store.

        Raises:
            ProxyResolveMissingKeyError: If the key associated with this
                factory does not exist in the store.
        """
        with Timer() as timer:
            store = self.get_store()
            obj = store.get(
                self.key,
                deserializer=self.deserializer,
                default=_MISSING_OBJECT,
            )

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
        """Asynchronously get object associated with key from store."""
        logger.debug('Starting asynchronous resolve of %s', self.key)
        self._obj_future = _get_default_pool().submit(self.resolve)


class PollingStoreFactory(StoreFactory[ConnectorT, T]):
    """Factory that polls a store until an object can be resolved.

    This is an extension of the
    [`StoreFactory`][proxystore.store.factory.StoreFactory] with the
    [`resolve()`][proxystore.store.factory.StoreFactory.resolve] method
    overridden to poll the store until the target object is available.

    Args:
        key: Key corresponding to object in store.
        store_config: Store configuration used to reinitialize the store if
            needed.
        deserializer: Optional callable used to deserialize the byte string.
            If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be used.
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
        deserializer: DeserializerT | None = None,
        evict: bool = False,
        polling: PollingPolicy | None = None,
    ) -> None:
        super().__init__(
            key,
            store_config,
            evict=evict,
            deserializer=deserializer,
        )
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
        # None, or None if the timeout was reached. Raises the exception if
        # an exception was set on the future.
        with Timer() as timer:
            store = self.get_store()
            deserializer = _deserialize_with_exceptions(
                self.deserializer
                if self.deserializer is not None
                else store.deserializer,
            )
            sleep_interval = self.polling.interval
            time_waited = 0.0

            while True:
                obj = store.get(
                    self.key,
                    deserializer=deserializer,
                    default=_MISSING_OBJECT,
                )

                # Break because we found the object or we hit the timeout
                if obj is not _MISSING_OBJECT or (
                    timeout is not None and time_waited >= timeout
                ):
                    break

                time.sleep(sleep_interval)
                time_waited += sleep_interval
                new_interval = sleep_interval * self.polling.backoff_factor
                sleep_interval = (
                    new_interval
                    if self.polling.interval_limit is None
                    else min(new_interval, self.polling.interval_limit)
                )

            if obj is _MISSING_OBJECT:
                return None
            if isinstance(obj, _FutureException):
                raise obj.exception
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

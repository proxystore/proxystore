"""Factory implementations."""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import cast
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeVar

import proxystore
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.types import ConnectorKeyT
from proxystore.store.types import ConnectorT
from proxystore.store.types import DeserializerT
from proxystore.utils.timer import Timer

if TYPE_CHECKING:
    from proxystore.store.base import Store

logger = logging.getLogger(__name__)

_default_pool = ThreadPoolExecutor()
_factory_get_store_lock = threading.Lock()
_MISSING_OBJECT = object()

T = TypeVar('T')


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
            [`resolve()`][proxystore.store.base.StoreFactory.resolve]
            is called.
        deserializer: Optional callable used to deserialize the byte string.
            If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be used.
    """

    def __init__(
        self,
        key: ConnectorKeyT,
        store_config: dict[str, Any],
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
            store.metrics.add_time('factory.call', self.key, timer.elapsed_ns)

        return obj

    def __getstate__(self) -> dict[str, Any]:
        # Override pickling behavior to not serialize a possible future
        state = self.__dict__.copy()
        state['_obj_future'] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)

    def get_store(self) -> Store[ConnectorT]:
        """Get store and reinitialize if necessary.

        Raises:
            ValueError: If the type of the returned store does not match the
                expected store type passed to the factory constructor.
        """
        with _factory_get_store_lock:
            store = proxystore.store.get_store(self.store_config['name'])
            if store is None:
                store = proxystore.store.Store.from_config(self.store_config)
                proxystore.store.register_store(store)
            return store

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
                )

            if self.evict:
                store.evict(self.key)

        if store.metrics is not None:
            total_time = timer.elapsed_ns
            store.metrics.add_time('factory.resolve', self.key, total_time)

        return cast(T, obj)

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from store."""
        logger.debug(f'Starting asynchronous resolve of {self.key}')
        self._obj_future = _default_pool.submit(self.resolve)


class PollingStoreFactory(StoreFactory[ConnectorT, T]):
    """Factory that polls a store until and object can be resolved.

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
            [`resolve()`][proxystore.store.base.StoreFactory.resolve]
            is called.
        polling_interval: Seconds to sleep between polling the store for the
            object.
        polling_timeout: Optional maximum number of seconds to poll for.
    """

    def __init__(
        self,
        key: ConnectorKeyT,
        store_config: dict[str, Any],
        *,
        deserializer: DeserializerT | None = None,
        evict: bool = False,
        polling_interval: float = 1,
        polling_timeout: float | None = None,
    ) -> None:
        super().__init__(
            key,
            store_config,
            evict=evict,
            deserializer=deserializer,
        )
        self._polling_interval = polling_interval
        self._polling_timeout = polling_timeout

    def resolve(self) -> T:
        """Get object associated with key from store.

        Raises:
            ProxyResolveMissingKeyError: If the object associated with the
                key is not available after `polling_timeout` seconds.
        """
        with Timer() as timer:
            store = self.get_store()
            time_waited = 0.0

            while True:
                obj = store.get(
                    self.key,
                    deserializer=self.deserializer,
                    default=_MISSING_OBJECT,
                )

                # Break because we found the object or we hit the timeout
                if obj is not _MISSING_OBJECT or (
                    self._polling_timeout is not None
                    and time_waited >= self._polling_timeout
                ):
                    break

                time.sleep(self._polling_interval)
                time_waited += self._polling_interval

            if obj is _MISSING_OBJECT:
                raise ProxyResolveMissingKeyError(
                    self.key,
                    type(store),
                    store.name,
                )
            elif self.evict:
                store.evict(self.key)

        if store.metrics is not None:
            total_time = timer.elapsed_ns
            store.metrics.add_time(
                'factory.polling_resolve',
                self.key,
                total_time,
            )

        return cast(T, obj)

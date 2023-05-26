"""Store implementation."""
from __future__ import annotations

import logging
import sys
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import Any
from typing import Callable
from typing import cast
from typing import Generic
from typing import Sequence
from typing import Tuple
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import proxystore
import proxystore.serialize
from proxystore.connectors.connector import Connector
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.store.cache import LRUCache
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.metrics import StoreMetrics
from proxystore.timer import Timer
from proxystore.utils import get_class_path
from proxystore.utils import import_class

_default_pool = ThreadPoolExecutor()
logger = logging.getLogger(__name__)

T = TypeVar('T')
ConnectorT = TypeVar('ConnectorT', bound=Connector[Any])
"""Connector type variable."""
ConnectorKeyT = Tuple[Any, ...]
"""Connector key type alias."""
SerializerT = Callable[[Any], bytes]
"""Serializer type alias."""
DeserializerT = Callable[[bytes], Any]
"""Deserializer type alias."""

_MISSING_OBJECT = object()


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
        metrics: Enable recording operation metrics.
    """

    def __init__(
        self,
        key: ConnectorKeyT,
        store_config: dict[str, Any],
        *,
        evict: bool = False,
        deserializer: DeserializerT | None = None,
        metrics: bool = False,
    ) -> None:
        self.key = key
        self.store_config = store_config
        self.evict = evict
        self.deserializer = deserializer
        self.metrics = metrics

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
        store = proxystore.store.get_store(self.store_config['name'])
        if store is None:
            store = Store.from_config(self.store_config)
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


class Store(Generic[ConnectorT]):
    """Key-value store interface for proxies.

    Tip:
        A [`Store`][proxystore.store.base.Store] instance can be used as a
        context manager which will automatically call
        [`close()`][proxystore.store.base.Store.close] on exit.

        ```python
        with Store('my-store', connector=...) as store:
            key = store.put('value')
            store.get(key)
        ```

    Args:
        name: Name of the store instance.
        connector: Connector instance to use for object storage.
        serializer: Optional callable which serializes the object. If `None`,
            the default serializer
            ([`serialize()`][proxystore.serialize.serialize]) will be used.
        deserializer: Optional callable used by the factory to deserialize the
            byte string. If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be
            used.
        cache_size: Size of LRU cache (in # of objects). If 0,
            the cache is disabled. The cache is local to the Python process.
        metrics: Enable recording operation metrics.

    Raises:
        ValueError: If `cache_size` is less than zero.
    """

    def __init__(
        self,
        name: str,
        connector: ConnectorT,
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
    ) -> None:
        if cache_size < 0:
            raise ValueError(
                f'Cache size cannot be negative. Got {cache_size}.',
            )

        self.connector = connector
        self.cache: LRUCache[ConnectorKeyT, Any] = LRUCache(cache_size)
        self._name = name
        self._metrics = StoreMetrics() if metrics else None
        self._cache_size = cache_size
        self._serializer = serializer
        self._deserializer = deserializer

        logger.info(f'Initialized {self}')

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        serializer = 'default' if self._serializer is None else 'custom'
        deserializer = 'default' if self._deserializer is None else 'custom'
        return (
            f'Store("{self.name}", connector={self.connector}, '
            f'serializer={serializer}, deserializer={deserializer}, '
            f'cache_size={self.cache.maxsize}, '
            f'metrics={self.metrics is not None})'
        )

    @property
    def name(self) -> str:
        """Name of this [`Store`][proxystore.store.base.Store] instance."""
        return self._name

    @property
    def metrics(self) -> StoreMetrics | None:
        """Optional metrics for this instance."""
        return self._metrics

    @property
    def serializer(self) -> SerializerT:
        """Serializer for this instance."""
        return (
            self._serializer
            if self._serializer is not None
            else proxystore.serialize.serialize
        )

    @property
    def deserializer(self) -> DeserializerT:
        """Deserializer for this instance."""
        return (
            self._deserializer
            if self._deserializer is not None
            else proxystore.serialize.deserialize
        )

    def close(self) -> None:
        """Close the connector associated with the store.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.
        """
        self.connector.close()

    def config(self) -> dict[str, Any]:
        """Get the store configuration.

        Example:
            ```python
            >>> store = Store(...)
            >>> config = store.config()
            >>> store = Store.from_config(config)
            ```

        Returns:
            Store configuration.
        """
        return {
            'name': self.name,
            'connector_type': get_class_path(type(self.connector)),
            'connector_config': self.connector.config(),
            'serializer': self._serializer,
            'deserializer': self._deserializer,
            'cache_size': self._cache_size,
            'metrics': self.metrics is not None,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Store[Any]:
        """Create a new store instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.

        Returns:
            Store instance.
        """
        config = config.copy()  # Avoid messing with callers version
        connector_type = config.pop('connector_type')
        connector_config = config.pop('connector_config')
        connector = import_class(connector_type)
        config['connector'] = connector.from_config(connector_config)
        return cls(**config)

    def evict(self, key: ConnectorKeyT) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        with Timer() as timer:
            with Timer() as connector_timer:
                self.connector.evict(key)

            if self.metrics is not None:
                ctime = connector_timer.elapsed_ns
                self.metrics.add_time('store.evict.connector', key, ctime)

            self.cache.evict(key)

        if self.metrics is not None:
            self.metrics.add_time('store.evict', key, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): EVICT {key} in '
            f'{timer.elapsed_ms:.3f} ms',
        )

    def exists(self, key: ConnectorKeyT) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        with Timer() as timer:
            res = self.cache.exists(key)
            if not res:
                with Timer() as connector_timer:
                    res = self.connector.exists(key)

                if self.metrics is not None:
                    ctime = connector_timer.elapsed_ns
                    self.metrics.add_time('store.exists.connector', key, ctime)

        if self.metrics is not None:
            self.metrics.add_time('store.exists', key, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): EXISTS {key} in '
            f'{timer.elapsed_ms:.3f} ms',
        )
        return res

    def get(
        self,
        key: ConnectorKeyT,
        *,
        deserializer: DeserializerT | None = None,
        default: object | None = None,
    ) -> Any | None:
        """Get the object associated with the key.

        Args:
            key: Key associated with the object to retrieve.
            deserializer: Optionally override the default deserializer for the
                store instance.
            default: An optional value to be returned if an object
                associated with the key does not exist.

        Returns:
            Object or `None` if the object does not exist.
        """
        timer = Timer()
        timer.start()

        if self.is_cached(key):
            value = self.cache.get(key)

            timer.stop()
            if self.metrics is not None:
                self.metrics.add_counter('store.get.cache_hits', key, 1)
                self.metrics.add_time('store.get', key, timer.elapsed_ns)

            logger.debug(
                f'Store(name="{self.name}"): GET {key} in '
                f'{timer.elapsed_ms:.3f} ms (cached=True)',
            )
            return value

        with Timer() as connector_timer:
            value = self.connector.get(key)

        if self.metrics is not None:
            ctime = connector_timer.elapsed_ns
            self.metrics.add_counter('store.get.cache_misses', key, 1)
            self.metrics.add_time('store.get.connector', key, ctime)

        if value is not None:
            with Timer() as deserializer_timer:
                if deserializer is not None:
                    result = deserializer(value)
                else:
                    result = self.deserializer(value)

            if self.metrics is not None:
                dtime = deserializer_timer.elapsed_ns
                obj_size = len(value)
                self.metrics.add_time('store.get.deserialize', key, dtime)
                self.metrics.add_attribute(
                    'store.get.object_size',
                    key,
                    obj_size,
                )

            self.cache.set(key, result)
        else:
            result = default

        timer.stop()
        if self.metrics is not None:
            self.metrics.add_time('store.get', key, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): GET {key} in '
            f'{timer.elapsed_ms:.3f} ms (cached=False)',
        )
        return result

    def is_cached(self, key: ConnectorKeyT) -> bool:
        """Check if an object associated with the key is cached locally.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If the object is cached.
        """
        return self.cache.exists(key)

    def proxy(
        self,
        obj: T,
        *,
        evict: bool = False,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> Proxy[T]:
        """Create a proxy that will resolve to an object in the store.

        Args:
            obj: The object to place in store and return a proxy for.
            evict: If the proxy should evict the object once resolved.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            kwargs: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.connector.Connector.put].

        Returns:
            A proxy of the object.
        """
        with Timer() as timer:
            key = self.put(obj, serializer=serializer, **kwargs)
            factory: StoreFactory[ConnectorT, T] = StoreFactory(
                key,
                store_config=self.config(),
                deserializer=deserializer,
                evict=evict,
                metrics=self.metrics is not None,
            )
            proxy = Proxy(factory)

        if self.metrics is not None:
            self.metrics.add_time('store.proxy', key, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): PROXY {key} in '
            f'{timer.elapsed_ms:.3f} ms',
        )
        return proxy

    def proxy_batch(
        self,
        objs: Sequence[T],
        *,
        evict: bool = False,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> list[Proxy[T]]:
        """Create proxies that will resolve to an object in the store.

        Args:
            objs: The objects to place in store and return a proxies for.
            evict: If a proxy should evict its object once resolved.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            kwargs: Additional keyword arguments to pass to
                [`Connector.put_batch()`][proxystore.connectors.connector.Connector.put_batch].

        Returns:
            A list of proxies of the objects.
        """
        with Timer() as timer:
            keys = self.put_batch(objs, serializer=serializer, **kwargs)
            proxies: list[Proxy[T]] = [
                self.proxy_from_key(
                    key,
                    evict=evict,
                    deserializer=deserializer,
                )
                for key in keys
            ]

        if self.metrics is not None:
            self.metrics.add_time('store.proxy_batch', keys, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): PROXY_BATCH ({len(proxies)} items) '
            f'in {timer.elapsed_ms:.3f} ms',
        )
        return proxies

    def proxy_from_key(
        self,
        key: ConnectorKeyT,
        *,
        evict: bool = False,
        deserializer: DeserializerT | None = None,
    ) -> Proxy[T]:
        """Create a proxy that will resolve to an object already in the store.

        Args:
            key: The key associated with an object already in the store.
            evict: If the proxy should evict the object once resolved.
            deserializer: Optionally override the default deserializer for the
                store instance.

        Returns:
            A proxy of the object.
        """
        factory: StoreFactory[ConnectorT, T] = StoreFactory(
            key,
            store_config=self.config(),
            deserializer=deserializer,
            evict=evict,
            metrics=self.metrics is not None,
        )
        logger.debug(f'Store(name="{self.name}"): PROXY_FROM_KEY {key}')
        return Proxy(factory)

    def locked_proxy(
        self,
        obj: T,
        *,
        evict: bool = False,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> ProxyLocker[T]:
        """Proxy an object and return [`ProxyLocker`][proxystore.proxy.ProxyLocker].

        Args:
            obj: The object to place in store and return a proxy for.
            evict: If the proxy should evict the object once resolved.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            kwargs: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.connector.Connector.put].

        Returns:
            A proxy wrapped in a [`ProxyLocker`][proxystore.proxy.ProxyLocker].
        """
        return ProxyLocker(
            self.proxy(
                obj,
                evict=evict,
                serializer=serializer,
                deserializer=deserializer,
                **kwargs,
            ),
        )

    def put(
        self,
        obj: Any,
        *,
        serializer: SerializerT | None = None,
        **kwargs: Any,
    ) -> ConnectorKeyT:
        """Put an object in the store.

        Args:
            obj: Object to put in the store.
            serializer: Optionally override the default serializer for the
                store instance.
            kwargs: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.connector.Connector.put].

        Returns:
            A key which can be used to retrieve the object.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        timer = Timer()
        timer.start()

        with Timer() as serialize_timer:
            if serializer is not None:
                obj = serializer(obj)
            else:
                obj = self.serializer(obj)

        if not isinstance(obj, bytes):
            raise TypeError('Serializer must produce bytes.')

        with Timer() as connector_timer:
            key = self.connector.put(obj, **kwargs)

        timer.stop()
        if self.metrics is not None:
            ctime = connector_timer.elapsed_ns
            stime = serialize_timer.elapsed_ns
            self.metrics.add_attribute('store.put.object_size', key, len(obj))
            self.metrics.add_time('store.put.serialize', key, stime)
            self.metrics.add_time('store.put.connector', key, ctime)
            self.metrics.add_time('store.put', key, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): PUT {key} in '
            f'{timer.elapsed_ms:.3f} ms',
        )
        return key

    def put_batch(
        self,
        objs: Sequence[Any],
        *,
        serializer: SerializerT | None = None,
        **kwargs: Any,
    ) -> list[ConnectorKeyT]:
        """Put multiple objects in the store.

        Args:
            objs: Sequence of objects to put in the store.
            serializer: Optionally override the default serializer for the
                store instance.
            kwargs: Additional keyword arguments to pass to
                [`Connector.put_batch()`][proxystore.connectors.connector.Connector.put_batch].

        Returns:
            A list of keys which can be used to retrieve the objects.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        timer = Timer()
        timer.start()

        def _serialize(obj: Any) -> bytes:
            if serializer is not None:
                obj = serializer(obj)
            else:
                obj = self.serializer(obj)

            if not isinstance(obj, bytes):
                raise TypeError('Serializer must produce bytes.')

            return obj

        with Timer() as serialize_timer:
            _objs = list(map(_serialize, objs))

        with Timer() as connector_timer:
            keys = self.connector.put_batch(_objs, **kwargs)

        timer.stop()
        if self.metrics is not None:
            ctime = connector_timer.elapsed_ns
            stime = serialize_timer.elapsed_ns
            sizes = sum(len(obj) for obj in _objs)
            self.metrics.add_attribute(
                'store.put_batch.object_sizes',
                keys,
                sizes,
            )
            self.metrics.add_time('store.put_batch.serialize', keys, stime)
            self.metrics.add_time('store.put_batch.connector', keys, ctime)
            self.metrics.add_time('store.put_batch', keys, timer.elapsed_ns)

        logger.debug(
            f'Store(name="{self.name}"): PUT_BATCH ({len(keys)} items) in '
            f'{timer.elapsed_ms:.3f} ms',
        )
        return keys

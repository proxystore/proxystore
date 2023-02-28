"""Base Store Abstract Class."""
from __future__ import annotations

import copy
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

import proxystore as ps
from proxystore.connectors.connector import Connector
from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import deserialize as default_deserializer
from proxystore.serialize import serialize as default_serializer
from proxystore.store.cache import LRUCache
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.stats import FunctionEventStats
from proxystore.store.stats import STORE_METHOD_KEY_IS_RESULT
from proxystore.store.stats import TimeStats
from proxystore.store.utils import get_key
from proxystore.utils import fullname
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


class StoreFactory(Factory[T], Generic[ConnectorT, T]):
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
        self.stats: FunctionEventStats | None = None
        if 'stats' in self.store_config and self.store_config['stats']:
            self.stats = FunctionEventStats()
            # Monkeypatch methods with wrappers to track their stats
            setattr(  # noqa: B010
                self,
                'resolve',
                self.stats.wrap(self.resolve, preset_key=self.key),
            )
            setattr(  # noqa: B010
                self,
                'resolve_async',
                self.stats.wrap(self.resolve_async, preset_key=self.key),
            )

    def __getnewargs_ex__(
        self,
    ) -> tuple[tuple[ConnectorKeyT, dict[str, Any]], dict[str, Any]]:
        # Pickle without possible futures.
        return (
            self.key,
            self.store_config,
        ), {
            'evict': self.evict,
            'deserializer': self.deserializer,
        }

    def _get_value(self) -> T:
        """Get the value associated with the key from the store."""
        store = self.get_store()
        obj = store.get(self.key, deserializer=self.deserializer)

        if obj is None:
            raise ProxyResolveMissingKeyError(
                self.key,
                type(store),
                store.name,
            )

        if self.evict:
            store.evict(self.key)

        return cast(T, obj)

    def _should_resolve_async(self) -> bool:
        """Check if it makes sense to do asynchronous resolution."""
        return not self.get_store().is_cached(self.key)

    def get_store(self) -> Store[ConnectorT]:
        """Get store and reinitialize if necessary.

        Raises:
            ValueError: If the type of the returned store does not match the
                expected store type passed to the factory constructor.
        """
        store = ps.store.get_store(self.store_config['name'])
        if store is None:
            store = Store.from_config(self.store_config)
            ps.store.register_store(store)
        return store

    def resolve(self) -> T:
        """Get object associated with key from store.

        Raises:
            ProxyResolveMissingKeyError: If the key associated with this
                factory does not exist in the store.
        """
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        return self._get_value()

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from store."""
        if self._should_resolve_async():
            self._obj_future = _default_pool.submit(self._get_value)


class Store(Generic[ConnectorT]):
    """Key-value store interface for proxies.

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
        stats: Collect stats on store operations.

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
        stats: bool = False,
    ) -> None:
        if cache_size < 0:
            raise ValueError(
                f'Cache size cannot be negative. Got {cache_size}.',
            )

        self.name = name
        self.connector = connector
        self.cache: LRUCache[ConnectorKeyT, Any] = LRUCache(cache_size)
        self._cache_size = cache_size
        self._serializer = serializer
        self._deserializer = deserializer

        self._stats: FunctionEventStats | None = None
        if stats:
            self._stats = FunctionEventStats()
            # Monkeypatch methods with wrappers to track their stats
            for attr in dir(self):
                if (
                    callable(getattr(self, attr))
                    and not attr.startswith('_')
                    and attr in STORE_METHOD_KEY_IS_RESULT
                ):
                    method = getattr(self, attr)
                    # For most method, the key is the first arg which wrap()
                    # expects by default, but there are a couple where the
                    # key is passed as a kwarg
                    wrapped = self._stats.wrap(
                        method,
                        key_is_result=STORE_METHOD_KEY_IS_RESULT[attr],
                    )
                    setattr(self, attr, wrapped)

        logger.debug(f'initialized {self}')

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
        s = f'{fullname(self.__class__)}('
        attributes = [
            f'{key}={value}'
            for key, value in self.__dict__.items()
            if not key.startswith('_') and not callable(value)
        ]
        attributes.sort()
        s += ', '.join(attributes)
        s += ')'
        return s

    @property
    def has_stats(self) -> bool:
        """Whether the store keeps track of performance stats."""
        return self._stats is not None

    @property
    def serializer(self) -> SerializerT:
        """Serializer for this instance."""
        return (
            self._serializer
            if self._serializer is not None
            else default_serializer
        )

    @property
    def deserializer(self) -> DeserializerT:
        """Deserializer for this instance."""
        return (
            self._deserializer
            if self._deserializer is not None
            else default_deserializer
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
            'stats': self._stats is not None,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Store[Any]:
        """Create a new store instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.

        Returns:
            Store instance.
        """
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
        self.connector.evict(key)
        self.cache.evict(key)
        logger.debug(
            f'evict called for key={key} and Store(name={self.name})',
        )

    def exists(self, key: ConnectorKeyT) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        res = self.connector.exists(key)
        logger.debug(
            f'exists called for key={key} and Store(name={self.name}): '
            f'result={res}',
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
        if self.is_cached(key):
            value = self.cache.get(key)
            logger.debug(
                f'get called for key={key} and Store(name={self.name}): '
                'was_cached=True',
            )
            return value

        value = self.connector.get(key)
        if value is not None:
            if deserializer is not None:
                value = deserializer(value)
            else:
                value = self.deserializer(value)
            self.cache.set(key, value)
            logger.debug(
                f'get called for key={key} and Store(name={self.name}): '
                'was_cached=False',
            )
            return value

        logger.debug(
            f'get called for key={key} and Store(name={self.name}): '
            'key did not exist',
        )
        return default

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
        key = self.set(obj, serializer=serializer, **kwargs)
        factory: StoreFactory[ConnectorT, T] = StoreFactory(
            key,
            store_config=self.config(),
            deserializer=deserializer,
            evict=evict,
        )
        logger.debug(
            f'proxy called for key={key} and Store(name={self.name})',
        )
        return Proxy(factory)

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
        keys = self.set_batch(objs, serializer=serializer, **kwargs)
        return [
            self.proxy_from_key(key, evict=evict, deserializer=deserializer)
            for key in keys
        ]

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
        logger.debug(
            f'proxy called for key={key} and Store(name={self.name})',
        )
        factory: StoreFactory[ConnectorT, T] = StoreFactory(
            key,
            store_config=self.config(),
            deserializer=deserializer,
            evict=evict,
        )
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

    def set(
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
        if serializer is not None:
            obj = serializer(obj)
        else:
            obj = default_serializer(obj)

        if not isinstance(obj, bytes):
            raise TypeError('Serializer must produce bytes.')

        key = self.connector.put(obj, **kwargs)

        logger.debug(
            f'set called for key={key} and Store(name={self.name})',
        )
        return key

    def set_batch(
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
        _objs: list[bytes] = []

        for obj in objs:
            if serializer is not None:
                obj = serializer(obj)
            else:
                obj = default_serializer(obj)

            if not isinstance(obj, bytes):
                raise TypeError('Serializer must produce bytes.')

            _objs.append(obj)

        keys = self.connector.put_batch(_objs, **kwargs)

        logger.debug(
            f'set called for keys={keys} and Store(name={self.name})',
        )
        return keys

    def stats(
        self,
        key_or_proxy: ConnectorKeyT | Proxy[T],
    ) -> dict[str, TimeStats]:
        """Get stats on the store.

        Args:
            key_or_proxy: A key to get stats for or a proxy to extract the key
                from.

        Returns:
            A dict with keys corresponding to method names and values which \
            are [`TimeStats`][proxystore.store.stats.TimeStats] instances \
            with the statistics for calls to the corresponding method with \
            the specified key.

        Example:
            ```python
            {
                "get": TimeStats(
                    calls=32,
                    avg_time_ms=0.0123,
                    min_time_ms=0.0012,
                    max_time_ms=0.1234,
                ),
                "set": TimeStats(...),
                "evict": TimeStats(...),
                ...
            }
            ```

        Raises:
            ValueError: If `self` was initialized with `#!python stats=False`.
        """
        if self._stats is None:
            raise ValueError(
                'Stats are not being tracked because this store was '
                'initialized with stats=False.',
            )
        stats = {}
        if isinstance(key_or_proxy, ps.proxy.Proxy):
            key = get_key(key_or_proxy)
            # Merge stats from the proxy into self
            if hasattr(key_or_proxy.__factory__, 'stats'):
                proxy_stats = key_or_proxy.__factory__.stats
                if proxy_stats is not None:
                    for event in proxy_stats:
                        stats[event.function] = copy.copy(proxy_stats[event])
        else:
            key = key_or_proxy

        for event in list(self._stats.keys()):
            if event.key == key:
                stats[event.function] = copy.copy(self._stats[event])
        return stats

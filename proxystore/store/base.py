"""Base Store Abstract Class."""
from __future__ import annotations

import copy
import logging
import sys
from abc import ABCMeta
from abc import abstractmethod
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import Any
from typing import Callable
from typing import cast
from typing import Generic
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import proxystore as ps
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

_default_pool = ThreadPoolExecutor()
logger = logging.getLogger(__name__)

T = TypeVar('T')
KeyT = TypeVar('KeyT', bound=NamedTuple)
SerializerT = Callable[[Any], bytes]
"""Serializer type alias."""
DeserializerT = Callable[[bytes], Any]
"""Deserializer type alias."""


class StoreFactory(Factory[T], Generic[KeyT, T]):
    """Base Factory for Stores.

    Adds support for asynchronously retrieving objects from a
    [`Store`][proxystore.store.base.Store].

    The factory takes the `store_type` and `store_kwargs` parameters that are
    used to reinitialize the store if the factory is sent to a remote
    process where the store has not already been initialized.

    Args:
        key: Key corresponding to object in store.
        store_type: Type of store this factory will resolve an object from.
        store_name: Name of store.
        store_kwargs: Optional keyword arguments used to reinitialize store.
        evict: If True, evict the object from the store once
            [`resolve()`][proxystore.store.base.StoreFactory.resolve]
            is called.
        deserializer: Optional callable used to deserialize the byte string.
            If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be used.
    """

    def __init__(
        self,
        key: KeyT,
        store_type: type[Store[KeyT]],
        store_name: str,
        store_kwargs: dict[str, Any] | None = None,
        *,
        evict: bool = False,
        deserializer: DeserializerT | None = None,
    ) -> None:
        self.key = key
        self.store_type = store_type
        self.store_name = store_name
        self.store_kwargs = {} if store_kwargs is None else store_kwargs
        self.evict = evict
        self.deserializer = deserializer

        # The following are not included when a factory is serialized
        # because they are specific to that instance of the factory
        self._obj_future: Future[T] | None = None
        self.stats: FunctionEventStats | None = None
        if 'stats' in self.store_kwargs and self.store_kwargs['stats'] is True:
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
    ) -> tuple[
        tuple[KeyT, type[Store[KeyT]], str, dict[str, Any]],
        dict[str, Any],
    ]:
        # Pickle without possible futures.
        return (
            self.key,
            self.store_type,
            self.store_name,
            self.store_kwargs,
        ), {
            'evict': self.evict,
            'deserializer': self.deserializer,
        }

    def _get_value(self) -> T:
        """Get the value associated with the key from the store."""
        store: Store[KeyT] = self.get_store()
        obj = store.get(self.key, deserializer=self.deserializer)

        if obj is None:
            raise ProxyResolveMissingKeyError(
                self.key,
                self.store_type,
                self.store_name,
            )

        if self.evict:
            store.evict(self.key)

        return cast(T, obj)

    def _should_resolve_async(self) -> bool:
        """Check if it makes sense to do asynchronous resolution."""
        return not self.get_store().is_cached(self.key)

    def get_store(self) -> Store[KeyT]:
        """Get store and reinitialize if necessary.

        Raises:
            ValueError: If the type of the returned store does not match the
                expected store type passed to the factory constructor.
        """
        store = ps.store.get_store(self.store_name)
        if store is None:
            store = self.store_type(self.store_name, **self.store_kwargs)
            ps.store.register_store(store)

        if not isinstance(store, self.store_type):
            raise ValueError(
                f'store_name={self.store_name} passed to '
                f'{type(self).__name__} does not correspond to store of '
                f'type {self.store_type.__name__}',
            )

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


class Store(Generic[KeyT], metaclass=ABCMeta):
    """Key-value store interface.

    Provides base functionality for interaction with an object store including
    serialization and caching.

    Subclasses of [`Store`][proxystore.store.base.Store] must implement
    [`create_key()`][proxystore.store.base.Store.create_key],
    [`evict()`][proxystore.store.base.Store.evict],
    [`exists()`][proxystore.store.base.Store.exists],
    [`get_bytes()`][proxystore.store.base.Store.get_bytes], and
    [`set_bytes()`][proxystore.store.base.Store.set_bytes]. Subclasses may
    implement [`close()`][proxystore.store.base.Store.close] if needed.

    The [`Store`][proxystore.store.base.Store] handles caching and stores all
    objects as key-bytestring pairs, i.e., objects passed to
    [`get()`][proxystore.store.base.Store.get] or
    [`set()`][proxystore.store.base.Store.set] will be
    appropriately (de)serialized before being passed to
    [`get_bytes()`][proxystore.store.base.Store.get_bytes] or
    [`set_bytes()`][proxystore.store.base.Store.set_bytes], respectively.

    Args:
        name: Name of the store instance.
        cache_size: Size of LRU cache (in # of objects). If 0,
            the cache is disabled. The cache is local to the Python process.
        stats: Collect stats on store operations.
        kwargs: Additional keyword arguments to return from
            [`Store.kwargs`][proxystore.store.base.Store.kwargs]. I.e., the
            additional keyword arguments needed to reinitialize this store.

    Raises:
        ValueError: If `cache_size` is less than zero.
    """

    def __init__(
        self,
        name: str,
        *,
        cache_size: int = 16,
        stats: bool = False,
        kwargs: dict[str, Any] | None,
    ) -> None:
        if cache_size < 0:
            raise ValueError(
                f'Cache size cannot be negative. Got {cache_size}.',
            )

        self.name = name

        self._cache: LRUCache[KeyT, Any] = LRUCache(cache_size)
        self._kwargs = {'stats': stats, 'cache_size': cache_size}
        if kwargs is not None:  # pragma: no branch
            self._kwargs.update(kwargs)

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

    @property
    def has_stats(self) -> bool:
        """Whether the store keeps track of performance stats."""
        return self._stats is not None

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
    def kwargs(self) -> dict[str, Any]:
        """Kwargs for this store instance."""
        return self._kwargs.copy()

    def close(self) -> None:
        """Cleanup any objects associated with the store.

        Many [`Store`][proxystore.store.base.Store] types do not have any
        objects that requiring cleaning up so this method a no-op by default
        unless overridden.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.
        """
        pass

    def create_key(self, obj: Any) -> KeyT:
        """Create key for the object.

        Args:
            obj: Object to be placed in store.

        Returns:
            A key.
        """
        raise NotImplementedError

    @abstractmethod
    def evict(self, key: KeyT) -> None:
        """Evict object associated with key.

        Args:
            key: The key corresponding to object in store to evict.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: KeyT) -> bool:
        """Check if key exists.

        Args:
            key: The key to check.

        Returns:
            If the key exists in the store.
        """
        raise NotImplementedError

    def get(
        self,
        key: KeyT,
        *,
        deserializer: DeserializerT | None = None,
        default: object | None = None,
    ) -> Any | None:
        """Return object associated with key.

        Args:
            key: The key corresponding to object.
            deserializer: Optional callable used to deserialize the
                byte string. If `None`, the default deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            default: Optionally provide value to be returned if an object
                associated with the key does not exist.

        Returns:
            The object associated with key or `default` if key does not exist.
        """
        if self.is_cached(key):
            value = self._cache.get(key)
            logger.debug(
                f"GET key='{key}' FROM {self.__class__.__name__}"
                f"(name='{self.name}'): was_cached=True",
            )
            return value

        value = self.get_bytes(key)
        if value is not None:
            if deserializer is not None:
                value = deserializer(value)
            else:
                value = default_deserializer(value)
            self._cache.set(key, value)
            logger.debug(
                f"GET key='{key}' FROM {self.__class__.__name__}"
                f"(name='{self.name}'): was_cached=False",
            )
            return value

        logger.debug(
            f"GET key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}'): key did not exist, returned default",
        )
        return default

    @abstractmethod
    def get_bytes(self, key: KeyT) -> bytes | None:
        """Get serialized object from remote store.

        Args:
            key: The key corresponding to the object.

        Returns:
            The serialized object or `None` if it does not exist.
        """
        raise NotImplementedError

    def is_cached(self, key: KeyT) -> bool:
        """Check if object is cached locally.

        Args:
            key: The key corresponding to the object.

        Returns:
            If the object associated with the key is cached.
        """
        return self._cache.exists(key)

    def proxy(
        self,
        obj: T,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> Proxy[T]:
        """Create a proxy that will resolve to an object in the store.

        Warning:
            If the factory requires reinstantiating the store to correctly
            resolve the object, the factory should reinstantiate the store
            with the same arguments used to instantiate the store that
            created the proxy/factory. I.e. the :func:`proxy()` function
            should pass any arguments given to :func:`Store.__init__()`
            along to the factory so the factory can correctly recreate the
            store if the factory is resolved in a different Python process.

        Args:
            obj: The object to place in store and return proxy for.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            kwargs: Additional arguments to pass to the Factory.

        Returns:
            A proxy of the object.
        """
        key = self.set(obj, serializer=serializer)
        logger.debug(
            f"PROXY key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        factory: StoreFactory[KeyT, T] = StoreFactory(
            key,
            store_type=type(self),
            store_name=self.name,
            store_kwargs=self.kwargs,
            deserializer=deserializer,
            **kwargs,
        )
        return Proxy(factory)

    def proxy_batch(
        self,
        objs: Sequence[T],
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> list[Proxy[T]]:
        """Create proxies for batch of objects in the store.

        See [`Store.proxy()`][proxystore.store.base.Store.proxy] for more
        details.

        Args:
            objs: The objects to place in store and return proxies for.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            kwargs: additional arguments to pass to the Factory.

        Returns:
            A list of proxies of the objects.
        """
        keys = self.set_batch(objs, serializer=serializer)
        return [
            self.proxy_from_key(key, deserializer=deserializer, **kwargs)
            for key in keys
        ]

    def proxy_from_key(
        self,
        key: KeyT,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> Proxy[T]:
        """Create a proxy to an object already in the store.

        Note:
            This method will not verify that the key is valid so an error
            will not be raised until the returned proxy is resolved.

        Args:
            key: The key corresponding to an object already in the store
                that will be the target object of the returned proxy.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            kwargs: Additional arguments to pass to the Factory.

        Returns:
            A proxy.
        """
        logger.debug(
            f"PROXY key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        factory: StoreFactory[KeyT, T] = StoreFactory(
            key,
            store_type=type(self),
            store_name=self.name,
            store_kwargs=self.kwargs,
            deserializer=deserializer,
            **kwargs,
        )
        return Proxy(factory)

    def locked_proxy(
        self,
        obj: T,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        **kwargs: Any,
    ) -> ProxyLocker[T]:
        """Create a proxy locker that will prevent resolution.

        Args:
            obj: The object to place in store and create proxy of.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            kwargs: Additional arguments to pass to the Factory.

        Returns:
            A proxy wrapped in a [`ProxyLocker`][proxystore.proxy.ProxyLocker].
        """
        return ProxyLocker(
            self.proxy(
                obj,
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
    ) -> KeyT:
        """Set key-object pair in store.

        Args:
            obj: The object to be placed in the store.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.

        Returns:
            A key that can be used to retrieve the object.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        if serializer is not None:
            obj = serializer(obj)
        else:
            obj = default_serializer(obj)

        if not isinstance(obj, bytes):
            raise TypeError('Serializer must produce bytes.')

        key = self.create_key(obj)
        self.set_bytes(key, obj)

        logger.debug(
            f"SET key='{key}' IN {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        return key

    def set_batch(
        self,
        objs: Sequence[Any],
        *,
        serializer: SerializerT | None = None,
    ) -> list[KeyT]:
        """Set objects in store.

        Args:
            objs: Iterable of objects to be placed in the store.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.

        Returns:
            List of keys that can be used to retrieve the objects.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        return [self.set(obj, serializer=serializer) for obj in objs]

    @abstractmethod
    def set_bytes(self, key: KeyT, data: bytes) -> None:
        """Set serialized object in remote store with key.

        Args:
            key: The key corresponding to the object.
            data: The serialized object.
        """
        raise NotImplementedError

    def stats(self, key_or_proxy: KeyT | Proxy[T]) -> dict[str, TimeStats]:
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

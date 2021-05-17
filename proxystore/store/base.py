"""Backend Store Abstract Classes"""
from __future__ import annotations

import time

from abc import ABC, abstractmethod
from typing import Any, Optional

import proxystore as ps
from proxystore.factory import Factory
from proxystore.store.cache import LRUCache


class Store(ABC):
    """Abstraction of a key-value store"""

    def __init__(self, name) -> None:
        """Init Store

        Args:
            name (str): name of the store instance.
        """
        self.name = name

    @abstractmethod
    def evict(self, key: str) -> None:
        """Evict object associated with key

        Args:
            key (str): key corresponding to object in store to evict.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        raise NotImplementedError

    @abstractmethod
    def get(
        self,
        key: str,
        *,
        strict: bool = False,
        default: Any = None,
    ) -> Optional[object]:
        """Return object associated with key

        Args:
            key (str): key corresponding to object.
            strict (bool): guarentee returned object is the most recent
                version (default: False).
            default: optionally provide value to be returned if an object
                associated with the key does not exist (default: None).

        Returns:
            object associated with key or `default` if key does not exist.
        """
        raise NotImplementedError

    @abstractmethod
    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object.
            strict (bool): guarentee object in cache is most recent version
                (default: False).

        Returns:
            `bool`
        """
        raise NotImplementedError

    @abstractmethod
    def proxy(
        self,
        obj: Optional[object] = None,
        key: Optional[str] = None,
        *,
        factory: Factory = Factory,
        **kwargs,
    ) -> 'ps.proxy.Proxy':
        """Create a proxy that will resolve to an object in the store

        Warning:
            If the factory requires reinstantiating the store to correctly
            resolve the object, the factory should reinstantiate the store
            with the same arguments used to instantiate the store that
            created the proxy/factory. I.e. the :func:`proxy()` function
            should pass any arguments given to :func:`Store.__init__()`
            along to the factory so the factory can correctly recreate the
            store if the factory is resolved in a different Python process.

        Args:
            obj (object): object to place in store and return proxy for.
                If an object is not provided, a key must be provided that
                corresponds to an object already in the store
                (default: None).
            key (str): optional key to associate with `obj` in the store.
                If not provided, a key will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxy. The factory class should be able
                to correctly resolve the object from this store
                (default: :any:`Factory <proxystore.factory.Factory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `key` and `obj` are both `None`.
            ValueError:
                if `obj` is None and `key` does not exist in the store.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, obj: Any) -> None:
        """Set key-object pair in store

        Args:
            key (str): key to use with the object.
            obj (object): object to be placed in the store.
        """
        raise NotImplementedError


class RemoteStore(Store, ABC):
    """Abstraction for interacting with a remote key-value store

    Provides base functionality for interaction with a remote store including
    serialization and caching.
    Subclasses of :class:`RemoteStore` must implement
    :func:`evict() <Store.evict()>`, :func:`exists() <Store.exists()>`,
    :func:`get_str()`, :func:`set_str()` and :func:`proxy() <Store.proxy()>`.
    The :class:`RemoteStore` handles the caching.

    :class:`RemoteStore` stores key-string pairs, i.e., objects passed to
    :func:`get()` or :func:`set()` will be appropriately (de)serialized.
    Functionality for serialized, caching, and strict guarentees are already
    provided in :func:`get()` and :func:`set()`.
    """

    def __init__(self, name: str, cache_size: int = 0) -> None:
        """Init RemoteStore

        Args:
            name (str): name of the store instance.
            cache_size (int): size of local cache (in # of objects). If 0,
                the cache is disabled (default: 0).

        Raises:
            ValueError:
                if `cache_size` is negative.
        """
        if cache_size < 0:
            raise ValueError('Cache size cannot be negative')
        self.name = name
        self.cache_size = cache_size
        self._cache = LRUCache(cache_size) if cache_size > 0 else None

    @abstractmethod
    def get_str(self, key: str) -> Optional[str]:
        """Get serialized object from remote store

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist.
        """
        raise NotImplementedError

    @abstractmethod
    def set_str(self, key: str, data: str) -> None:
        """Set serialized object in remote store with key

        Args:
            key (str): key corresponding to object.
            data (str): serialized object.
        """
        raise NotImplementedError

    def get(
        self,
        key: str,
        *,
        deserialize: bool = True,
        strict: bool = False,
        default: Optional[object] = None,
    ) -> Optional[object]:
        """Return object associated with key

        Args:
            key (str): key corresponding to object.
            deserialize (bool): deserialize object if True. If objects
                are custom serialized, set this as False (default: True).
            strict (bool): guarentee returned object is the most recent
                version (default: False).
            default: optionally provide value to be returned if an object
                associated with the key does not exist (default: None).

        Returns:
            object associated with key or `default` if key does not exist.
        """
        if self.is_cached(key, strict=strict):
            return self._cache.get(key)[1]

        value = self.get_str(key)
        if value is not None:
            timestamp = float(self.get_str(key + '_timestamp'))
            if deserialize:
                value = ps.serialize.deserialize(value)
            if self._cache is not None:
                self._cache.set(key, (timestamp, value))
            return value

        return default

    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object.
            strict (bool): guarentee object in cache is most recent version
                (default: False).

        Returns:
            bool
        """
        if self._cache is None:
            return False

        if self._cache.exists(key):
            if strict:
                store_timestamp = float(self.get_str(key + '_timestamp'))
                cache_timestamp = self._cache.get(key)[0]
                return cache_timestamp >= store_timestamp
            return True

        return False

    def set(self, key: str, obj: Any, *, serialize: bool = True) -> None:
        """Set key-object pair in store

        Args:
            key (str): key to use with the object.
            obj (object): object to be placed in the store.
            serialize (bool): serialize object if True. If object is already
                custom serialized, set this as False (default: True).
        """
        if serialize:
            obj = ps.serialize.serialize(obj)

        self.set_str(key, obj)
        self.set_str(key + '_timestamp', str(time.time()))

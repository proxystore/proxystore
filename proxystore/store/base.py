"""Base Store Abstract Class"""
from __future__ import annotations

import logging

from abc import ABCMeta, abstractmethod
from typing import Any, Iterable, List, Optional

import proxystore as ps
from proxystore.factory import Factory

logger = logging.getLogger(__name__)


class Store(metaclass=ABCMeta):
    """Abstraction of a key-value store"""

    def __init__(self, name) -> None:
        """Init Store

        Args:
            name (str): name of the store instance.
        """
        self.name = name
        logger.debug(f"Initialized {self}")

    def __repr__(self) -> None:
        """String representation of Store instance"""
        s = f"{ps.utils.fullname(self.__class__)}("
        attributes = [
            f"{key}={value}"
            for key, value in self.__dict__.items()
            if not key.startswith('_') and not callable(value)
        ]
        attributes.sort()
        s += ", ".join(attributes)
        s += ")"
        return s

    def cleanup(self) -> None:
        """Cleanup any objects associated with the store

        Many :class:`Store <.Store>` types do not have any objects that
        requiring cleaning up so this method is simply a no-op.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.
        """
        pass

    def create_key(self, obj: Any) -> str:
        """Create key for the object

        Args:
            obj: object to be placed in store.

        Returns:
            key (str)
        """
        return ps.utils.create_key(obj)

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
        *,
        key: Optional[str] = None,
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
    def proxy_batch(
        self,
        objs: Optional[Iterable[Optional[object]]] = None,
        *,
        keys: Optional[Iterable[Optional[str]]] = None,
        factory: Factory = Factory,
        **kwargs,
    ) -> List['ps.proxy.Proxy']:
        """Create proxies for batch of objects in the store

        See :any:`proxy() <proxystore.store.base.Store.proxy>` for more
        details.

        Args:
            objs (Iterable[object]): objects to place in store and return
                proxies for. If an iterable of objects is not provided, an
                iterable of keys must be provided that correspond to objects
                already in the store (default: None).
            keys (Iterable[str]): optional keys to associate with `objs` in the
                store. If not provided, keys will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxies. The factory class should be able
                to correctly resolve an object from this store
                (default: :any:`Factory <proxystore.factory.Factory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            List of :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `keys` and `objs` are both `None`.
            ValueError:
                if `objs` is None and `keys` does not exist in the store.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, obj: Any, *, key: Optional[str] = None) -> str:
        """Set key-object pair in store

        Args:
            obj (object): object to be placed in the store.
            key (str, optional): key to use with the object. If the key is not
                provided, one will be created.

        Returns:
            key (str). Note that some implementations of a store may return
            a key different from the provided key.
        """
        raise NotImplementedError

    @abstractmethod
    def set_batch(
        self,
        objs: Iterable[Any],
        *,
        keys: Optional[Iterable[Optional[str]]] = None,
    ) -> List[str]:
        """Set objects in store

        Args:
            objs (Iterable[object]): iterable of objects to be placed in the
                store.
            keys (Iterable[str], optional): keys to use with the objects.
                If the keys are not provided, keys will be created.

        Returns:
            List of keys (str). Note that some implementations of a store may
            return keys different from the provided keys.

        Raises:
            ValueError:
                if :code:`keys is not None` and :code:`len(objs) != len(keys)`.
        """
        raise NotImplementedError

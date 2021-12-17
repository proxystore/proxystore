"""Base Store Abstract Class"""
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Any, Optional

import proxystore as ps
from proxystore.factory import Factory


class Store(metaclass=ABCMeta):
    """Abstraction of a key-value store"""

    def __init__(self, name) -> None:
        """Init Store

        Args:
            name (str): name of the store instance.
        """
        self.name = name

    def __del__(self) -> None:
        self.cleanup()

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


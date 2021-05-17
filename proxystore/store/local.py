"""LocalStore Implementation"""
from __future__ import annotations

from typing import Any, Optional

import proxystore as ps
from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.store.base import Store


class LocalFactory(Factory):
    """Factory for LocalStore

    The :class:`LocalFactory <.LocalFactory>` stores a key, and when called,
    the :class:`LocalFactory <.LocalFactory>` returns the object associated with
    the key in the :any:`LocalStore <proxystore.store.local.LocalStore>`.
    """

    def __init__(self, key: str, name: str, *, evict: bool = False) -> None:
        """Init LocalFactory

        Args:
            key (str): key corresponding to object in store.
            name (str): name of store that created this factory.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).

        Raises:
            RuntimeError:
                if :func:`resolve` is called but a LocalStore
                has not been initialized.
        """
        self.key = key
        self.name = name
        self.evict = evict

    def resolve(self) -> Any:
        """Resolve and return object from store"""
        store = ps.store.get_store(self.name)
        if store is None:
            raise RuntimeError(
                'LocalStore is not initalized, cannot resolve factory'
            )
        obj = store.get(self.key)
        if self.evict:
            store.evict(self.key)
        return obj

    def __getnewargs_ex__(self):
        """Helper method for pickling"""
        return (self.key, self.name), {'evict': self.evict}


class LocalStore(Store):
    """Local Memory Key-Object Store"""

    def __init__(self, name: str) -> None:
        """Init Store

        Args:
            name (str): name of this store instance.
        """
        self._store = {}
        super(LocalStore, self).__init__(name)

    def evict(self, key: str) -> None:
        """Evict object associated with key

        Args:
            key (str): key corresponding to object in store to evict.
        """
        if key in self._store:
            del self._store[key]

    def exists(self, key: str) -> bool:
        """Check if key exists

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        return key in self._store

    def get(
        self,
        key: str,
        *,
        strict: bool = False,
        default: Optional[object] = None,
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
        if key in self._store:
            return self._store[key]
        return default

    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object.
            strict (bool): guarentee object in cache is most recent version
                (default: False).

        Returns:
            `bool`
        """
        return key in self._store

    def proxy(
        self,
        obj: Optional[object] = None,
        key: Optional[str] = None,
        *,
        factory: Factory = LocalFactory,
        **kwargs,
    ) -> 'proxystore.proxy.Proxy':  # noqa: F821
        """Create a proxy that will resolve to an object in the store

        Args:
            obj (object): object to place in store and return proxy for.
                If an object is not provided, a key must be provided that
                corresponds to an object already in the store (default: None).
            key (str): optional key to associate with `obj` in the store.
                If not provided, a key will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxy. The factory class should be able
                to correctly resolve the object from this store
                (default: :class:`LocalFactory <.LocalFactory>`).
            kwargs (dict): additional arguments to pass to the factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `key` and `obj` are both `None`.
            ValueError:
                if `obj` is None and `key` does not exist in the store.
        """
        if key is None and obj is None:
            raise ValueError('At least one of key or obj must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            self.set(key, obj)
        elif not self.exists(key):
            raise ValueError(
                f'An object with key {key} does not exist in the store'
            )
        return Proxy(factory(key=key, name=self.name, **kwargs))

    def set(self, key: str, obj: Any) -> None:
        """Set key-object pair in store

        Args:
            key (str): key to use with the object.
            obj (object): object to be placed in the store.
        """
        self._store[key] = obj

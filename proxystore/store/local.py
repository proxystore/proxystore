"""LocalStore Implementation."""
from __future__ import annotations

import logging
from typing import Any
from typing import Sequence

import proxystore as ps
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.base import StoreFactory

logger = logging.getLogger(__name__)


class LocalFactory(StoreFactory):
    """Factory for LocalStore."""

    def __init__(
        self,
        key: str,
        store_name: str,
        store_kwargs: dict[str, Any] | None = None,
        *,
        evict: bool = False,
    ) -> None:
        """Init LocalFactory.

        Args:
            key (str): key corresponding to object in store.
            store_name (str): name of store
            store_kwargs (dict): optional keyword arguments used to
                reinitialize store.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
        """
        super().__init__(
            key,
            store_type=LocalStore,
            store_name=store_name,
            store_kwargs=store_kwargs,
            evict=evict,
        )

    def get_store(self) -> Store:
        """Get store and raise RuntimeError if it is missing.

        Raises:
            RuntimeError:
                if :func:`resolve` is called but a LocalStore
                has not been initialized.
            ValueError:
                if the type of the returned store does not match the expected
                store type passed to the factory constructor.
        """
        store = ps.store.get_store(self.store_name)
        if store is None:
            raise RuntimeError(
                f'LocalStore with name {self.store_name} does not exist.',
            )

        return super().get_store()


class LocalStore(Store):
    """Local Memory Key-Object Store."""

    def __init__(self, name: str, **kwargs: Any) -> None:
        """Init LocalStore.

        Args:
            name (str): name of this store instance.
            kwargs (dict): additional keyword arguments to pass to
                :class:`Store <proxystore.store.base.Store>`.
        """
        self._store: dict[str, Any] = {}
        super().__init__(name, **kwargs)

    def _kwargs(
        self,
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Helper for handling inheritance with kwargs property.

        Args:
            kwargs (optional, dict): dict to use as return object. If None,
                a new dict will be created.
        """
        return super()._kwargs(kwargs)

    def evict(self, key: str) -> None:
        """Evict object associated with key.

        Args:
            key (str): key corresponding to object in store to evict.
        """
        if key in self._store:
            del self._store[key]
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        """Check if key exists.

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
        default: Any | None = None,
    ) -> Any | None:
        """Return object associated with key.

        Args:
            key (str): key corresponding to object.
            strict (bool): guarantee returned object is the most recent
                version (default: False).
            default: optionally provide value to be returned if an object
                associated with the key does not exist (default: None).

        Returns:
            object associated with key or `default` if key does not exist.
        """
        if key in self._store:
            logger.debug(
                f"GET key='{key}' FROM {self.__class__.__name__}"
                f"(name='{self.name}')",
            )
            return self._store[key]
        logger.debug(
            f"GET key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}'): key does not exists, returned default",
        )
        return default

    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        """Check if object is cached locally.

        Args:
            key (str): key corresponding to object.
            strict (bool): guarantee object in cache is most recent version
                (default: False).

        Returns:
            `bool`
        """
        return key in self._store

    def proxy(  # type: ignore[override]
        self,
        obj: Any | None = None,
        *,
        key: str | None = None,
        factory: type[LocalFactory] = LocalFactory,
        **kwargs: Any,
    ) -> ps.proxy.Proxy:
        """Create a proxy that will resolve to an object in the store.

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
        """
        if key is None and obj is None:
            raise ValueError('At least one of key or obj must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            self.set(obj, key=key)
        logger.debug(
            f"PROXY key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        return Proxy(
            factory(
                key=key,
                store_name=self.name,
                store_kwargs=self.kwargs,
                **kwargs,
            ),
        )

    def proxy_batch(  # type: ignore[override]
        self,
        objs: Sequence[Any] | None = None,
        *,
        keys: Sequence[str] | None = None,
        factory: type[LocalFactory] | None = None,
        **kwargs: Any,
    ) -> list[ps.proxy.Proxy]:
        """Create proxies for batch of objects in the store.

        See :any:`proxy() <proxystore.store.base.Store.proxy>` for more
        details.

        Args:
            objs (Sequence[Any]): objects to place in store and return
                proxies for. If an iterable of objects is not provided, an
                iterable of keys must be provided that correspond to objects
                already in the store (default: None).
            keys (Sequence[str]): optional keys to associate with `objs` in the
                store. If not provided, keys will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxies. The factory class should be able
                to correctly resolve an object from this store. Defaults to
                None such that the default of :func:`proxy()` is used
                (default: None).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            List of :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `keys` and `objs` are both `None`.
            ValueError:
                if `objs` is None and `keys` does not exist in the store.
        """
        if objs is not None:
            final_keys = self.set_batch(objs, keys=keys)
        elif keys is not None:
            final_keys = list(keys)
        else:
            raise ValueError('At least one of key or obj must be specified')
        return [self.proxy(None, key=key, **kwargs) for key in final_keys]

    def set(self, obj: Any, *, key: str | None = None) -> str:
        """Set key-object pair in store.

        Args:
            obj (object): object to be placed in the store.
            key (str, optional): key to use with the object. If the key is not
                provided, one will be created.

        Returns:
            key (str)
        """
        if key is None:
            key = ps.utils.create_key(obj)
        self._store[key] = obj
        logger.debug(
            f"SET key='{key}' IN {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        return key

    def set_batch(
        self,
        objs: Sequence[Any],
        *,
        keys: Sequence[str | None] | None = None,
    ) -> list[str]:
        """Set objects in store.

        Args:
            objs (Sequence[Any]): iterable of objects to be placed in the
                store.
            keys (Sequence[str], optional): keys to use with the objects.
                If the keys are not provided, keys will be created.

        Returns:
            List of keys (str). Note that some implementations of a store may
            return keys different from the provided keys.

        Raises:
            ValueError:
                if :code:`keys is not None` and :code:`len(objs) != len(keys)`.
        """
        if keys is not None and len(objs) != len(keys):
            raise ValueError(
                f'objs has length {len(objs)} but keys has length {len(keys)}',
            )
        if keys is None:
            keys = [None] * len(objs)

        return [self.set(obj, key=key) for key, obj in zip(keys, objs)]

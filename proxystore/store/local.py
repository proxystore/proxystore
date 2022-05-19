"""LocalStore Implementation."""
from __future__ import annotations

import logging
from typing import Any

import proxystore as ps
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
        serialize: bool = True,
        strict: bool = False,
    ) -> None:
        """Init LocalFactory.

        Args:
            key (str): key corresponding to object in store.
            store_name (str): name of store
            store_kwargs (dict): optional keyword arguments used to
                reinitialize store.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            serialize (bool): if True, object in store is serialized and
                should be deserialized upon retrieval (default: True).
            strict (bool): guarantee object produce when this object is called
                is the most recent version of the object associated with the
                key in the store (default: False).
        """
        super().__init__(
            key,
            LocalStore,
            store_name,
            store_kwargs,
            evict=evict,
            serialize=serialize,
            strict=strict,
        )


class LocalStore(Store):
    """Local Memory Key-Object Store."""

    def __init__(self, name: str, **kwargs: Any) -> None:
        """Init LocalStore.

        Args:
            name (str): name of this store instance.
            kwargs (dict): additional keyword arguments to pass to
                :class:`Store <proxystore.store.base.Store>`. Note,
                `cache_size` will be set to 0.
        """
        self._store: dict[str, bytes] = {}
        kwargs['cache_size'] = 0
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

    def get_bytes(self, key: str) -> bytes | None:
        return self._store.get(key, None)

    def get_timestamp(self, key: str) -> float:
        """Get timestamp of most recent object version in the store."""
        return 0

    def proxy(  # type: ignore[override]
        self,
        obj: Any | None = None,
        *,
        key: str | None = None,
        factory: type[LocalFactory] = LocalFactory,
        **kwargs: Any,
    ) -> ps.proxy.Proxy:
        return super().proxy(obj, key=key, factory=factory, **kwargs)

    def set_bytes(self, key: str, data: bytes) -> None:
        self._store[key] = data

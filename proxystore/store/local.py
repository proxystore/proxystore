"""LocalStore Implementation."""
from __future__ import annotations

import logging
import time
from typing import Any

from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class LocalStore(Store):
    """Local Memory Key-Object Store."""

    def __init__(
        self,
        name: str,
        *,
        store_dict: dict[str, bytes] | None = None,
        **kwargs: Any,
    ) -> None:
        """Init LocalStore.

        Warning:
            :class:`LocalStore <.LocalStore>` should typically be used for
            testing proxystore locally as using proxy store within the same
            Python process is unnecessary.

        Args:
            name (str): name of this store instance.
            store_dict (dict): dictionary to store data in. If not specified,
                a new empty dict will be generated (default: None).
            kwargs (dict): additional keyword arguments to pass to
                :class:`Store <proxystore.store.base.Store>`.
        """
        self._store: dict[str, bytes] = {}
        if store_dict is not None:
            self._store = store_dict

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
        if kwargs is None:
            kwargs = {}
        kwargs.update({'store_dict': self._store})
        return super()._kwargs(kwargs)

    def evict(self, key: str) -> None:
        if key in self._store:
            del self._store[key]
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        return key in self._store

    def get_bytes(self, key: str) -> bytes | None:
        return self._store.get(key, None)

    def get_timestamp(self, key: str) -> float:
        return float(self._store[key + '_timestamp'].decode())

    def set_bytes(self, key: str, data: bytes) -> None:
        self._store[key + '_timestamp'] = str(time.time()).encode()
        self._store[key] = data

"""LocalStore Implementation."""
from __future__ import annotations

import logging
from typing import Any

from proxystore.store.base import Store

logger = logging.getLogger(__name__)


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

    def set_bytes(self, key: str, data: bytes) -> None:
        self._store[key] = data

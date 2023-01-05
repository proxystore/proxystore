"""LocalStore Implementation."""
from __future__ import annotations

import logging
from typing import Any
from typing import NamedTuple

import proxystore.utils as utils
from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class LocalStoreKey(NamedTuple):
    """Key to objects in a LocalStore."""

    id: str
    """Unique object ID."""


class LocalStore(Store[LocalStoreKey]):
    """Local Memory Key-Object Store."""

    def __init__(
        self,
        name: str,
        *,
        store_dict: dict[LocalStoreKey, bytes] | None = None,
        cache_size: int = 16,
        stats: bool = False,
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
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        self._store: dict[LocalStoreKey, bytes] = {}
        if store_dict is not None:
            self._store = store_dict

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'store_dict': self._store},
        )

    def create_key(self, obj: Any) -> LocalStoreKey:
        return LocalStoreKey(id=utils.create_key(obj))

    def evict(self, key: LocalStoreKey) -> None:
        if key in self._store:
            del self._store[key]
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: LocalStoreKey) -> bool:
        return key in self._store

    def get_bytes(self, key: LocalStoreKey) -> bytes | None:
        return self._store.get(key, None)

    def set_bytes(self, key: LocalStoreKey, data: bytes) -> None:
        self._store[key] = data

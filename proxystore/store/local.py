"""LocalStore Implementation."""
from __future__ import annotations

import logging
import time

from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class LocalStore(Store):
    """Local Memory Key-Object Store."""

    def __init__(
        self,
        name: str,
        *,
        store_dict: dict[str, bytes] | None = None,
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
        self._store: dict[str, bytes] = {}
        if store_dict is not None:
            self._store = store_dict

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'store_dict': self._store},
        )

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

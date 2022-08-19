"""MargoStore Implementation."""
from __future__ import annotations

import logging
import time
import pickle

import redis

# currently in chameleon-ps-rdma repo
from peer_service import RDMAClient as RDMA

from proxystore.store.base import Store

logger = logging.getLogger(__name__)

class MargoStore(Store):
    """Margo backend class."""

    def __init__(
        self,
        name: str,
        *,
        host: str,
        port: int,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init MargoStore.

        Args:
            name (str): name of the store instance.
            host (str): the IP address to launch the Margo server on (e.g., Infiniband IP).
            port (int): the desired port for the Margo server
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        self.host = host
        self.port = port
        self._margo = RDMA(host=self.host, port=self.port)
        
        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'host': self.host, 'port': self.port },
        )

    def evict(self, key: str) -> None:
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        return bool(self._margo.exists(key))

    def get_bytes(self, key: str) -> bytes | None:
        return self._margo.get(key)

    def get_timestamp(self, key: str) -> float:
        value = self._margo.get(key + '_timestamp')
        if value is None:
            raise KeyError(f"Key='{key}' does not exist in Redis store")
        return float(value.decode())

    def set_bytes(self, key: str, data: bytes) -> None:
        # We store the creation time for the key as a separate key-value.
        self._margo.set(key + '_timestamp', str(time.time()).encode())
        self._margo.set(key, data)

    def close(self):
        self._margo.close()

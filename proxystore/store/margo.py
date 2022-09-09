"""MargoStore Implementation."""
from __future__ import annotations
from genericpath import getsize

import logging
from sys import getsizeof
from typing import Any
from typing import NamedTuple

# currently in chameleon-ps-rdma repo
from peer_service import RDMAClient as RDMA

import proxystore.utils as utils
from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class MargoStoreKey(NamedTuple):
    """Key to objects in a MargoStore"""

    margo_key: str
    obj_size: int
    peer: RDMA.Peer


class MargoStore(Store[MargoStoreKey]):
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
            kwargs={"host": self.host, "port": self.port},
        )

    def create_key(self, obj: Any) -> MargoStoreKey:
        return MargoStoreKey(
            margo_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=RDMA.Peer(self._margo.addr, self._margo.provider_id),
        )

    def evict(self, key: MargoStoreKey) -> None:
        self._cache.evict(key.margo_key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}" f"(name='{self.name}')",
        )

    def exists(self, key: MargoStoreKey) -> bool:
        return bool(self._margo.exists(key.margo_key, peer=key.peer))

    def get_bytes(self, key: MargoStoreKey) -> bytes | None:
        return self._margo.get(key.margo_key, key.obj_size, peer=key.peer)

    def set_bytes(self, key: MargoStoreKey, data: bytes) -> None:
        # We store the creation time for the key as a separate key-value.
        self._margo.set(key.margo_key, data, peer=key.peer)

    def close(self):
        self._margo.close()

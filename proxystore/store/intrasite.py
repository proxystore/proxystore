"""MargoStore Implementation."""
from __future__ import annotations
from genericpath import getsize

import logging
from sys import getsizeof
from typing import Any
from typing import NamedTuple
from proxystore.serialize import serialize

from proxystore_rdma.peers import PeerClient

import proxystore.utils as utils
from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class IntrasiteStoreKey(NamedTuple):
    """Key to objects in a MargoStore"""

    is_key: str
    obj_size: int
    peer: PeerClient.Peer


class IntrasiteStore(Store[IntrasiteStoreKey]):
    """Margo backend class."""

    def __init__(
        self,
        name: str,
        *,
        interface: str,
        port: int,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init MargoStore.

        Args:
            name (str): name of the store instance.
            interface (str): the network interface to use.
            port (int): the desired port for the Margo server
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        self.interface = interface
        self.port = port
        self._peer = PeerClient(interface=self.interface, port=self.port)

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={"interface": self.interface, "port": self.port, "serialize": False},
        )

    def create_key(self, obj: Any) -> IntrasiteStoreKey:
        return IntrasiteStoreKey(
            is_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=PeerClient.Peer(self._peer.addr, self._peer.provider_id),
        )

    def evict(self, key: IntrasiteStoreKey) -> None:
        self._cache.evict(key.is_key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}" f"(name='{self.name}')",
        )

    def exists(self, key: IntrasiteStoreKey) -> bool:
        return bool(self._peer.exists(key.is_key, peer=key.peer))

    def get_bytes(self, key: IntrasiteStoreKey) -> bytes | None:
        return self._peer.get(key.is_key, key.obj_size, peer=key.peer)

    def set_bytes(self, key: IntrasiteStoreKey, data: bytes) -> None:
        # We store the creation time for the key as a separate key-value.
        self._peer.set(key.is_key, data)

    def close(self):
        self._peer.close()

"""RedisStore Implementation."""
from __future__ import annotations

import logging
import time

import redis

from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class RedisStore(Store):
    """Redis backend class."""

    def __init__(
        self,
        name: str,
        *,
        hostname: str,
        port: int,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init RedisStore.

        Args:
            name (str): name of the store instance.
            hostname (str): Redis server hostname.
            port (int): Redis server port.
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        self.hostname = hostname
        self.port = port
        self._redis_client = redis.StrictRedis(host=hostname, port=port)
        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'hostname': self.hostname, 'port': self.port},
        )

    def evict(self, key: str) -> None:
        self._redis_client.delete(key)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        return bool(self._redis_client.exists(key))

    def get_bytes(self, key: str) -> bytes | None:
        return self._redis_client.get(key)

    def get_timestamp(self, key: str) -> float:
        value = self._redis_client.get(key + '_timestamp')
        if value is None:
            raise KeyError(f"Key='{key}' does not exist in Redis store")
        return float(value.decode())

    def set_bytes(self, key: str, data: bytes) -> None:
        # We store the creation time for the key as a separate redis key-value.
        self._redis_client.set(key + '_timestamp', time.time())
        self._redis_client.set(key, data)

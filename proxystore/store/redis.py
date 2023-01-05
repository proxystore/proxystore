"""RedisStore Implementation."""
from __future__ import annotations

import logging
from typing import Any
from typing import NamedTuple

import redis

import proxystore.utils as utils
from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class RedisStoreKey(NamedTuple):
    """Key to objects in a RedisStore."""

    redis_key: str
    """Unique object ID."""


class RedisStore(Store[RedisStoreKey]):
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

    def create_key(self, obj: Any) -> RedisStoreKey:
        return RedisStoreKey(redis_key=utils.create_key(obj))

    def evict(self, key: RedisStoreKey) -> None:
        self._redis_client.delete(key.redis_key)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: RedisStoreKey) -> bool:
        return bool(self._redis_client.exists(key.redis_key))

    def get_bytes(self, key: RedisStoreKey) -> bytes | None:
        return self._redis_client.get(key.redis_key)

    def set_bytes(self, key: RedisStoreKey, data: bytes) -> None:
        self._redis_client.set(key.redis_key, data)

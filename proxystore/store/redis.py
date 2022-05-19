"""RedisStore Implementation."""
from __future__ import annotations

import logging
import time
from typing import Any

import redis  # type: ignore

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
        **kwargs: Any,
    ) -> None:
        """Init RedisStore.

        Args:
            name (str): name of the store instance.
            hostname (str): Redis server hostname.
            port (int): Redis server port.
            kwargs (dict): additional keyword arguments to pass to
                :class:`Store <proxystore.store.base.Store>`.
        """
        self.hostname = hostname
        self.port = port
        self._redis_client = redis.StrictRedis(host=hostname, port=port)
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
        kwargs.update({'hostname': self.hostname, 'port': self.port})
        return super()._kwargs(kwargs)

    def evict(self, key: str) -> None:
        """Evict object associated with key from Redis.

        Args:
            key (str): key corresponding to object in store to evict.
        """
        self._redis_client.delete(key)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        """Check if key exists in Redis.

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        return self._redis_client.exists(key)

    def get_bytes(self, key: str) -> bytes | None:
        """Get serialized object from Redis.

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist.
        """
        return self._redis_client.get(key)

    def get_timestamp(self, key: str) -> float:
        """Get timestamp of most recent object version in the store.

        Args:
            key (str): key corresponding to object.

        Returns:
            timestamp (float) of when key was added to redis (seconds since
            epoch).

        Raises:
            KeyError:
                if `key` does not exist in store.
        """
        value = self._redis_client.get(key + '_timestamp')
        if value is None:
            raise KeyError(f"Key='{key}' does not exist in Redis store")
        return float(value.decode())

    def set_bytes(self, key: str, data: bytes) -> None:
        """Set serialized object in Redis with key.

        Args:
            key (str): key corresponding to object.
            data (bytes): serialized object.
        """
        if not isinstance(data, bytes):
            raise TypeError(f'data must be of type bytes. Found {type(data)}')
        # We store the creation time for the key as a separate redis key-value.
        self._redis_client.set(key + '_timestamp', time.time())
        self._redis_client.set(key, data)

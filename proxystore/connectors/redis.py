"""Redis connector implementation."""
from __future__ import annotations

import sys
import uuid
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import redis


class RedisKey(NamedTuple):
    """Key to objects store in a Redis server."""

    redis_key: str
    """Unique object ID."""


class RedisConnector:
    """Redis server connector.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
    """

    def __init__(self, hostname: str, port: int) -> None:
        self.hostname = hostname
        self.port = port
        self._redis_client = redis.StrictRedis(host=hostname, port=port)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(hostname={self.hostname}, '
            f'port={self.port})'
        )

    def close(self) -> None:
        """Close the connector and clean up."""
        pass

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {'hostname': self.hostname, 'port': self.port}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> RedisConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: RedisKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        self._redis_client.delete(key.redis_key)

    def exists(self, key: RedisKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        return bool(self._redis_client.exists(key.redis_key))

    def get(self, key: RedisKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        return self._redis_client.get(key.redis_key)

    def get_batch(self, keys: Sequence[RedisKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return self._redis_client.mget([key.redis_key for key in keys])

    def put(self, obj: bytes) -> RedisKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = RedisKey(redis_key=str(uuid.uuid4()))
        self._redis_client.set(key.redis_key, obj)
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[RedisKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.
        """
        keys = [RedisKey(redis_key=str(uuid.uuid4())) for _ in objs]
        self._redis_client.mset(
            {key.redis_key: obj for key, obj in zip(keys, objs)},
        )
        return keys

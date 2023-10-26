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
    """Key to objects store in a Redis server.

    Attributes:
        redis_key: Unique object ID.
    """

    redis_key: str


class RedisConnector:
    """Redis server connector.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
        clear: Remove all keys from the Redis server when
            [`close()`][proxystore.connectors.redis.RedisConnector.close]
            is called. This will delete keys regardless of if they were
            created by ProxyStore or not.
    """

    def __init__(self, hostname: str, port: int, clear: bool = False) -> None:
        self.hostname = hostname
        self.port = port
        self.clear = clear
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

    def close(self, clear: bool | None = None) -> None:
        """Close the connector and clean up.

        Warning:
            Passing `clear=True` will result in **ALL** keys in the Redis
            server being deleted regardless of if they were created by
            ProxyStore or not.

        Args:
            clear: Remove all keys in the Redis server. Overrides the default
                value of `clear` provided when the
                [`RedisConnector`][proxystore.connectors.redis.RedisConnector]
                was instantiated.
        """
        if self.clear if clear is None else clear:
            self._redis_client.flushdb()
        self._redis_client.close()

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'hostname': self.hostname,
            'port': self.port,
            'clear': self.clear,
        }

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
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return self._redis_client.mget([key.redis_key for key in keys])

    def new_key(self, obj: bytes | None = None) -> RedisKey:
        """Create a new key.

        Args:
            obj: Optional object which the key will be associated with.
                Ignored in this implementation.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.redis.RedisConnector.set] \
            has been called on the key.
        """
        return RedisKey(redis_key=str(uuid.uuid4()))

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
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        keys = [RedisKey(redis_key=str(uuid.uuid4())) for _ in objs]
        self._redis_client.mset(
            {key.redis_key: obj for key, obj in zip(keys, objs)},
        )
        return keys

    def set(self, key: RedisKey, obj: bytes) -> None:
        """Set the object associated with a key.

        Note:
            The [`Connector`][proxystore.connectors.protocols.Connector]
            provides write-once, read-many semantics. Thus,
            [`set()`][proxystore.connectors.redis.RedisConnector.set]
            should only be called once per key, otherwise unexpected behavior
            can occur.

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        self._redis_client.set(key.redis_key, obj)

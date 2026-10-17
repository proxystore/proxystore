"""Redis connector implementation."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import cast
from typing import NamedTuple
from typing import Self

import redis

from proxystore._compat import init_kwargs
from proxystore.serialize import BytesLike


class RedisKey(NamedTuple):
    """Key to objects stored in a Redis server.

    Attributes:
        redis_key: Unique object ID.
    """

    redis_key: str


class RedisConnector:
    """Redis server connector.

    Example:
        Connect to a Redis server which requires a password and TLS.

        ```python
        from proxystore.connectors.redis import RedisConnector

        connector = RedisConnector(
            'redis.example.com',
            6380,
            client_options={
                'username': 'proxystore',
                'password': '...',
                'ssl': True,
                'ssl_ca_certs': '/path/to/ca.pem',
            },
        )
        ```

    Warning:
        The `client_options` are part of the
        [`config()`][proxystore.connectors.redis.RedisConnector.config] of
        the connector, so a password in them is included in every proxy
        made by a [`Store`][proxystore.store.base.Store] using this
        connector, and in a
        [`StoreConfig`][proxystore.store.config.StoreConfig] written to a
        TOML file. Anyone who can read a proxy (e.g., in logs or a task
        database) or the file can read the password.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
        clear: Remove all keys from the Redis server when
            [`close()`][proxystore.connectors.redis.RedisConnector.close]
            is called. This will delete keys regardless of if they were
            created by ProxyStore or not.
        client_options: Keyword arguments passed to the
            [`redis.Redis`](https://redis.readthedocs.io/en/stable/connections.html#redis.Redis){target=_blank}
            client, such as `username`, `password`, `db`, `ssl`, and the
            other `ssl_*` options. The values must be picklable so proxies
            can be sent to other processes, and must be types TOML supports
            (e.g., not `None`) to write a
            [`StoreConfig`][proxystore.store.config.StoreConfig] to a file.

    Raises:
        ValueError: If `client_options` contains `host` or `port`, which
            are set by `hostname` and `port`, or `decode_responses`, because
            the connector stores bytes.
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        clear: bool = False,
        client_options: dict[str, Any] | None = None,
    ) -> None:
        client_options = dict(client_options or {})
        conflicts = sorted({'host', 'port'} & client_options.keys())
        if conflicts:
            raise ValueError(
                f'The client options cannot contain {conflicts}. Use the '
                'hostname and port arguments instead.',
            )
        if 'decode_responses' in client_options:
            raise ValueError(
                'The client options cannot contain decode_responses because '
                'the connector stores bytes.',
            )
        self.hostname = hostname
        self.port = port
        self.clear = clear
        self.client_options = client_options
        self._redis_client = redis.StrictRedis(
            host=hostname,
            port=port,
            **client_options,
        )

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

    def close(self, *, clear: bool | None = None) -> bool:
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

        Returns:
            If the keys were removed.
        """
        clear = self.clear if clear is None else clear
        if clear:
            self._redis_client.flushdb()
        self._redis_client.close()
        return clear

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'hostname': self.hostname,
            'port': self.port,
            'clear': self.clear,
            'client_options': dict(self.client_options),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> RedisConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**init_kwargs(cls, config))

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

    def get(self, key: RedisKey) -> BytesLike | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        # Responses are bytes because decode_responses is not allowed.
        return cast(BytesLike | None, self._redis_client.get(key.redis_key))

    def get_batch(self, keys: Sequence[RedisKey]) -> list[BytesLike | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        if len(keys) == 0:
            # Redis does not accept MGET without any keys.
            return []
        return cast(
            list[BytesLike | None],
            self._redis_client.mget([key.redis_key for key in keys]),
        )

    def new_key(self, obj: BytesLike | None = None) -> RedisKey:
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

    def put(self, obj: BytesLike) -> RedisKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = RedisKey(redis_key=str(uuid.uuid4()))
        self._redis_client.set(key.redis_key, bytes(obj))
        return key

    def put_batch(self, objs: Sequence[BytesLike]) -> list[RedisKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        if len(objs) == 0:
            # Redis does not accept MSET without any keys.
            return []
        keys = [RedisKey(redis_key=str(uuid.uuid4())) for _ in objs]
        self._redis_client.mset(
            {
                key.redis_key: bytes(obj)
                for key, obj in zip(keys, objs, strict=True)
            },
        )
        return keys

    def set(self, key: RedisKey, obj: BytesLike) -> None:
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
        self._redis_client.set(key.redis_key, bytes(obj))

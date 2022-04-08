"""RedisStore Implementation."""
from __future__ import annotations

import logging
import time
from typing import Any

import redis  # type: ignore

import proxystore as ps
from proxystore.store.remote import RemoteFactory
from proxystore.store.remote import RemoteStore

logger = logging.getLogger(__name__)


class RedisFactory(RemoteFactory):
    """Factory for Instances of RedisStore.

    Adds support for asynchronously retrieving objects from a
    :class:`RedisStore <.RedisStore>` backend and optional, strict guarantees
    on object versions.

    The factory takes the `store_type` and `store_args` parameters that are
    used to reinitialize the backend store if the factory is sent to a remote
    process backend has not already been initialized.
    """

    def __init__(
        self,
        key: str,
        store_name: str,
        store_kwargs: dict[str, Any] | None = None,
        *,
        evict: bool = False,
        serialize: bool = True,
        strict: bool = False,
    ) -> None:
        """Init RedisFactory.

        Args:
            key (str): key corresponding to object in store.
            store_name (str): name of store.
            store_kwargs (dict): optional keyword arguments used to
                reinitialize store.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            serialize (bool): if True, object in store is serialized and
                should be deserialized upon retrieval (default: True).
            strict (bool): guarantee object produce when this object is called
                is the most recent version of the object associated with the
                key in the store (default: False).
        """
        super().__init__(
            key,
            RedisStore,
            store_name,
            store_kwargs,
            evict=evict,
            serialize=serialize,
            strict=strict,
        )


class RedisStore(RemoteStore):
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
                :class:`RemoteStore <proxystore.store.remote.RemoteStore>`.
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

    def proxy(  # type: ignore[override]
        self,
        obj: Any | None = None,
        *,
        key: str | None = None,
        factory: type[RemoteFactory] = RedisFactory,
        **kwargs: Any,
    ) -> ps.proxy.Proxy:
        """Create a proxy that will resolve to an object in the store.

        Args:
            obj (object): object to place in store and return proxy for.
                If an object is not provided, a key must be provided that
                corresponds to an object already in the store (default: None).
            key (str): optional key to associate with `obj` in the store.
                If not provided, a key will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxy. The factory class should be able
                to correctly resolve the object from this store
                (default: :class:`RedisFactory <.RedisFactory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `key` and `obj` are both `None`.
        """
        return super().proxy(obj, key=key, factory=factory, **kwargs)

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

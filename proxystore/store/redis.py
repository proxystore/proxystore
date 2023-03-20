"""RedisStore Implementation."""
from __future__ import annotations

import warnings

from proxystore.connectors.redis import RedisConnector
from proxystore.store.base import DeserializerT
from proxystore.store.base import SerializerT
from proxystore.store.base import Store


class RedisStore(Store[RedisConnector]):
    """Store wrapper for Redis servers.

    Warning:
        This wrapper exists for backwards compatibility with ProxyStore
        <=0.4.* and will be deprecated in version 0.6.0.

    Args:
        name: Name of the store instance.
        hostname: Redis server hostname.
        port: Redis server port.
        serializer: Optional callable which serializes the object. If `None`,
            the default serializer
            ([`serialize()`][proxystore.serialize.serialize]) will be used.
        deserializer: Optional callable used by the factory to deserialize the
            byte string. If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be
            used.
        cache_size: Size of LRU cache (in # of objects). If 0,
            the cache is disabled. The cache is local to the Python process.
        metrics: Enable recording operation metrics.
    """

    def __init__(
        self,
        name: str,
        hostname: str,
        port: int,
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
    ) -> None:
        warnings.warn(
            'The RedisStore will be deprecated in v0.6.0. Initializing a '
            'Store with a Connector is preferred. See '
            'https://github.com/proxystore/proxystore/issues/214 for details.',
            DeprecationWarning,
            stacklevel=2,
        )
        connector = RedisConnector(hostname, port)
        super().__init__(
            name,
            connector,
            serializer=serializer,
            deserializer=deserializer,
            cache_size=cache_size,
            metrics=metrics,
        )

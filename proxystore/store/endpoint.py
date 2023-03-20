"""EndpointStore Implementation."""
from __future__ import annotations

import uuid
import warnings
from typing import Sequence

from proxystore.connectors.endpoint import EndpointConnector
from proxystore.store.base import DeserializerT
from proxystore.store.base import SerializerT
from proxystore.store.base import Store


class EndpointStore(Store[EndpointConnector]):
    """Store wrapper for ProxyStore Endpoints.

    Warning:
        This wrapper exists for backwards compatibility with ProxyStore
        <=0.4.* and will be deprecated in version 0.6.0.

    Args:
        name: Name of the store instance (default: None).
        endpoints: Sequence of valid and running endpoint
            UUIDs to use. At least one of these endpoints must be
            accessible by this process.
        proxystore_dir: Optionally specify the proxystore home
            directory. Defaults to [`home_dir()`[proxystore.utils.home_dir].
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
        endpoints: Sequence[str | uuid.UUID],
        proxystore_dir: str | None = None,
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
    ) -> None:
        warnings.warn(
            'The EndpointStore will be deprecated in v0.6.0. Initializing a '
            'Store with a Connector is preferred. See '
            'https://github.com/proxystore/proxystore/issues/214 for details.',
            DeprecationWarning,
            stacklevel=2,
        )
        connector = EndpointConnector(
            endpoints=endpoints,
            proxystore_dir=proxystore_dir,
        )
        super().__init__(
            name,
            connector,
            serializer=serializer,
            deserializer=deserializer,
            cache_size=cache_size,
            metrics=metrics,
        )

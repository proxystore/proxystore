"""MultiStore Implementation."""
from __future__ import annotations

import warnings
from typing import Any

from proxystore.connectors.connector import Connector
from proxystore.connectors.multi import MultiConnector
from proxystore.connectors.multi import Policy
from proxystore.store.base import DeserializerT
from proxystore.store.base import SerializerT
from proxystore.store.base import Store


class MultiStore(Store[MultiConnector]):
    """Store wrapper for `MultiConnector`.

    Warning:
        This wrapper exists for backwards compatibility with ProxyStore
        <=0.4.* and will be deprecated in version 0.6.0.

    Args:
        name: Name of the store instance.
        connectors: Mapping of names to tuples of a
            [`Connector`][proxystore.connectors.connector.Connector] and
            [`Policy`][proxystore.store.multi.Policy].
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
        connectors: dict[str, tuple[Connector[Any], Policy]],
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
    ) -> None:
        warnings.warn(
            'The MultiStore will be deprecated in v0.6.0. Initializing a '
            'Store with a Connector is preferred. See '
            'https://github.com/proxystore/proxystore/issues/214 for details.',
            DeprecationWarning,
            stacklevel=2,
        )
        connector = MultiConnector(connectors)
        super().__init__(
            name,
            connector,
            serializer=serializer,
            deserializer=deserializer,
            cache_size=cache_size,
            metrics=metrics,
        )

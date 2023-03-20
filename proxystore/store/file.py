"""FileStore Implementation."""
from __future__ import annotations

import warnings

from proxystore.connectors.file import FileConnector
from proxystore.store.base import DeserializerT
from proxystore.store.base import SerializerT
from proxystore.store.base import Store


class FileStore(Store[FileConnector]):
    """Store wrapper for shared filesystems.

    Warning:
        This wrapper exists for backwards compatibility with ProxyStore
        <=0.4.* and will be deprecated in version 0.6.0.

    Args:
        name: Name of the store instance.
        store_dir: Path to directory to store data in. Note this
            directory will be deleted upon closing the store.
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
        store_dir: str,
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
    ) -> None:
        warnings.warn(
            'The FileStore will be deprecated in v0.6.0. Initializing a '
            'Store with a Connector is preferred. See '
            'https://github.com/proxystore/proxystore/issues/214 for details.',
            DeprecationWarning,
            stacklevel=2,
        )
        connector = FileConnector(store_dir)
        super().__init__(
            name,
            connector,
            serializer=serializer,
            deserializer=deserializer,
            cache_size=cache_size,
            metrics=metrics,
        )

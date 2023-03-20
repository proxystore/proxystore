"""GlobusStore Implementation."""
from __future__ import annotations

import sys
import warnings

if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
    from typing import Literal
else:  # pragma: <3.9 cover
    from typing_extensions import Literal

from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.globus import GlobusEndpoint
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.store.base import DeserializerT
from proxystore.store.base import SerializerT
from proxystore.store.base import Store


class GlobusStore(Store[GlobusConnector]):
    """Store wrapper for Globus transfers.

    Warning:
        This wrapper exists for backwards compatibility with ProxyStore
        <=0.4.* and will be deprecated in version 0.6.0.

    Args:
        name: Name of the store instance.
        endpoints: Globus endpoints to keep in sync. If passed as a `dict`,
            the dictionary must match the format expected by
            [`GlobusEndpoints.from_dict()`][proxystore.store.globus.GlobusEndpoints.from_dict].
        polling_interval: Interval in seconds to check if Globus
            tasks have finished.
        sync_level: Globus transfer sync level.
        timeout: Timeout in seconds for waiting on Globus tasks.
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
        endpoints: GlobusEndpoints
        | list[GlobusEndpoint]
        | dict[str, dict[str, str]],
        polling_interval: int = 1,
        sync_level: int
        | Literal['exists', 'size', 'mtime', 'checksum'] = 'mtime',
        timeout: int = 60,
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
    ) -> None:
        warnings.warn(
            'The GlobusStore will be deprecated in v0.6.0. Initializing a '
            'Store with a Connector is preferred. See '
            'https://github.com/proxystore/proxystore/issues/214 for details.',
            DeprecationWarning,
            stacklevel=2,
        )
        connector = GlobusConnector(
            endpoints=endpoints,
            polling_interval=polling_interval,
            sync_level=sync_level,
            timeout=timeout,
        )
        super().__init__(
            name,
            connector,
            serializer=serializer,
            deserializer=deserializer,
            cache_size=cache_size,
            metrics=metrics,
        )

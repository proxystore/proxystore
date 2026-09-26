"""Endpoint implementation."""

from __future__ import annotations

import logging
from collections.abc import Generator
from types import TracebackType
from typing import Any

from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import log_name
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import Storage

logger = logging.getLogger(__name__)


class Endpoint:
    """ProxyStore Endpoint.

    An endpoint is an object store with `get`/`set` functionality.

    Example:
        ```python
        async with Endpoint('ep1', endpoint_id) as endpoint:
            serialized_data = b'data string'
            await endpoint.set('key', serialized_data)
            assert await endpoint.get('key') == serialized_data
            await endpoint.evict('key')
            assert not await endpoint.exists('key')
        ```

    Note:
        Endpoints can be configured and started via the
        [`proxystore-endpoint`](../cli.md#proxystore-endpoint) command-line
        interface.

    Args:
        name: Readable name of the endpoint.
        endpoint_id: ID of the endpoint.
        storage: Storage interface to use. If `None`,
            [`DictStorage`][proxystore.endpoint.storage.DictStorage] is used.
    """

    def __init__(
        self,
        name: str,
        endpoint_id: EndpointId,
        *,
        storage: Storage | None = None,
    ) -> None:
        self._name = name
        self._id = endpoint_id
        self._storage = DictStorage() if storage is None else storage
        self._closed = False

        logger.info('%s: initialized endpoint', self._log_prefix)

    @property
    def _log_prefix(self) -> str:
        return f'{type(self).__name__}[{log_name(self.id, self.name)}]'

    @property
    def name(self) -> str:
        """Name of this endpoint."""
        return self._name

    @property
    def id(self) -> EndpointId:
        """ID of this endpoint."""
        return self._id

    async def __aenter__(self) -> Endpoint:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def __await__(self) -> Generator[Any, None, Endpoint]:
        return self.__aenter__().__await__()

    def _check_local(self, endpoint: EndpointId | None) -> None:
        if endpoint is not None and endpoint != self.id:
            raise PeeringNotAvailableError(
                f'Cannot forward request to endpoint {endpoint} because '
                'peering is not available.',
            )

    async def evict(
        self, key: str, endpoint: EndpointId | None = None
    ) -> None:
        """Evict key from endpoint.

        Args:
            key: Key to evict.
            endpoint: Endpoint to perform operation on. If unspecified, the
                operation is performed on the local endpoint.

        Raises:
            PeeringNotAvailableError: If `endpoint` is not this endpoint.
        """
        logger.debug(
            '%s: EVICT key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        self._check_local(endpoint)
        await self._storage.evict(key)

    async def exists(
        self, key: str, endpoint: EndpointId | None = None
    ) -> bool:
        """Check if key exists on endpoint.

        Args:
            key: Key to check.
            endpoint: Endpoint to perform operation on. If unspecified, the
                operation is performed on the local endpoint.

        Returns:
            If the key exists.

        Raises:
            PeeringNotAvailableError: If `endpoint` is not this endpoint.
        """
        logger.debug(
            '%s: EXISTS key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        self._check_local(endpoint)
        return await self._storage.exists(key)

    async def get(
        self,
        key: str,
        endpoint: EndpointId | None = None,
    ) -> bytes | bytearray | None:
        """Get value associated with key on endpoint.

        Args:
            key: Key to get value for.
            endpoint: Endpoint to perform operation on. If unspecified, the
                operation is performed on the local endpoint.

        Returns:
            Value associated with key.

        Raises:
            PeeringNotAvailableError: If `endpoint` is not this endpoint.
        """
        logger.debug(
            '%s: GET key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        self._check_local(endpoint)
        return await self._storage.get(key, None)

    async def set(
        self,
        key: str,
        data: bytes | bytearray,
        endpoint: EndpointId | None = None,
    ) -> None:
        """Set key with data on endpoint.

        Args:
            key: Key to associate with value.
            data: Value to associate with key.
            endpoint: Endpoint to perform operation on. If unspecified, the
                operation is performed on the local endpoint.

        Raises:
            ObjectSizeExceededError: If the max object size is configured and
                the data exceeds that size.
            PeeringNotAvailableError: If `endpoint` is not this endpoint.
        """
        logger.debug(
            '%s: SET key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        self._check_local(endpoint)
        await self._storage.set(key, data)

    async def close(self) -> None:
        """Close the endpoint.

        This is idempotent so it is safe to call multiple times.
        """
        if self._closed:
            return
        self._closed = True
        await self._storage.close()
        logger.info('%s: endpoint closed', self._log_prefix)

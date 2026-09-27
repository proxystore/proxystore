"""Endpoint implementation."""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from types import TracebackType
from typing import Any
from typing import TYPE_CHECKING

from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import Storage

if TYPE_CHECKING:
    from proxystore.p2p.manager import PeerManager

logger = logging.getLogger(__name__)


class Endpoint:
    """ProxyStore Endpoint.

    An endpoint is an object store with `get`/`set` functionality.

    By default, an endpoint operates in isolation. If initialized with a
    [`PeerManager`][proxystore.p2p.manager.PeerManager], the endpoint can
    forward operations to peer endpoints by passing the ID of the peer as
    the `endpoint` argument of an operation. See the
    [`proxystore.p2p`][proxystore.p2p] module to learn more about peering.

    Warning:
        Requests made to remote endpoints will only invoke the request on
        the remote and return the result. I.e., invoking GET on a remote
        will return the value but will not store it on the local endpoint.

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

    Note:
        If the endpoint has a peer manager, the endpoint must be used as a
        context manager or initialized with await so the peer manager is
        started.

    Args:
        name: Readable name of the endpoint.
        endpoint_id: ID of the endpoint.
        peer_manager: Optional peer manager used to communicate with peer
            endpoints. The manager is closed when the endpoint is closed.
        storage: Storage interface to use. If `None`,
            [`DictStorage`][proxystore.endpoint.storage.DictStorage] is used.

    Raises:
        ValueError: If the ID of the peer manager does not match
            `endpoint_id`.
    """

    def __init__(
        self,
        name: str,
        endpoint_id: EndpointId,
        *,
        peer_manager: PeerManager | None = None,
        storage: Storage | None = None,
    ) -> None:
        if peer_manager is not None and peer_manager.id != endpoint_id:
            raise ValueError(
                f'The ID of the peer manager ({peer_manager.id}) does not '
                f'match the ID of the endpoint ({endpoint_id}).',
            )
        self._name = name
        self._id = endpoint_id
        self._peer_manager = peer_manager
        self._storage = DictStorage() if storage is None else storage
        self._closed = False

        logger.info(
            '%s: initialized endpoint (peering %s)',
            self._log_prefix,
            'disabled' if peer_manager is None else 'enabled',
        )

    @property
    def _log_prefix(self) -> str:
        return f'{type(self).__name__}[{self.id.log_name(self.name)}]'

    @property
    def name(self) -> str:
        """Name of this endpoint."""
        return self._name

    @property
    def id(self) -> EndpointId:
        """ID of this endpoint."""
        return self._id

    @property
    def peer_manager(self) -> PeerManager | None:
        """Peer manager used to communicate with peer endpoints."""
        return self._peer_manager

    async def async_init(self) -> None:
        """Start the peer manager, if one was provided.

        This is idempotent so it is safe to call multiple times.
        """
        if self._peer_manager is not None:
            await self._peer_manager.start(self._handle_peer_request)

    async def __aenter__(self) -> Endpoint:
        await self.async_init()
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

    def _is_peer_request(self, endpoint: EndpointId | None) -> bool:
        if endpoint is None or endpoint == self.id:
            return False
        if self._peer_manager is None:
            raise PeeringNotAvailableError(
                f'Cannot forward request to endpoint {endpoint} because '
                'peering is not enabled.',
            )
        return True

    async def _request_peer(
        self,
        endpoint: EndpointId,
        op: Op,
        meta: dict[str, Any],
        data: bytes | bytearray | None = None,
    ) -> tuple[Status, dict[str, Any], bytes | bytearray]:
        assert self._peer_manager is not None
        logger.debug(
            '%s: sending %s request with meta=%s to %s',
            self._log_prefix,
            op.name,
            meta,
            endpoint,
        )
        code, meta, response_data = await self._peer_manager.request(
            endpoint,
            op,
            meta,
            data,
        )

        if code in (Status.OK, Status.NOT_FOUND):
            return Status(code), meta, response_data
        error = meta.get('error', 'no error message')
        if code == Status.TOO_LARGE:
            raise ObjectSizeExceededError(f'Peer {endpoint}: {error}')
        raise PeerRequestError(f'Request to peer {endpoint} failed: {error}')

    async def _handle_peer_request(
        self,
        peer: EndpointId,
        op: int,
        meta: dict[str, Any],
        data: bytes | bytearray,
    ) -> tuple[int, dict[str, Any] | None, bytes | bytearray | None]:
        """Handle a request from a peer endpoint on the local storage."""
        if op == Op.PING:
            return Status.OK, None, None
        try:
            request = Request.from_meta(meta)
        except EndpointProtocolError as e:
            return Status.BAD_REQUEST, {'error': str(e)}, None
        if request.endpoint is not None:
            # Requests from peers are never forwarded to another peer.
            return (
                Status.BAD_REQUEST,
                {'error': 'requests from peers cannot be forwarded'},
                None,
            )

        logger.debug(
            '%s: received op %s request with key=%s from %s',
            self._log_prefix,
            op,
            request.key,
            peer,
        )
        key = request.key
        try:
            if op == Op.GET:
                result = await self._storage.get(key, None)
                if result is None:
                    return Status.NOT_FOUND, None, None
                return Status.OK, None, result
            if op == Op.SET:
                await self._storage.set(key, data)
                return Status.OK, None, None
            if op == Op.EXISTS:
                exists = await self._storage.exists(key)
                return Status.OK, {'exists': exists}, None
            if op == Op.EVICT:
                await self._storage.evict(key)
                return Status.OK, None, None
        except ObjectSizeExceededError as e:
            return Status.TOO_LARGE, {'error': str(e)}, None
        return Status.BAD_REQUEST, {'error': f'unknown op {op}'}, None

    async def evict(
        self,
        key: str,
        endpoint: EndpointId | None = None,
    ) -> None:
        """Evict key from endpoint.

        Args:
            key: Key to evict.
            endpoint: Endpoint to perform operation on. If unspecified, the
                operation is performed on the local endpoint.

        Raises:
            PeeringNotAvailableError: If `endpoint` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: EVICT key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            await self._request_peer(
                endpoint, Op.EVICT, Request(key).to_meta()
            )
        else:
            await self._storage.evict(key)

    async def exists(
        self,
        key: str,
        endpoint: EndpointId | None = None,
    ) -> bool:
        """Check if key exists on endpoint.

        Args:
            key: Key to check.
            endpoint: Endpoint to perform operation on. If unspecified, the
                operation is performed on the local endpoint.

        Returns:
            If the key exists.

        Raises:
            PeeringNotAvailableError: If `endpoint` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: EXISTS key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            _, meta, _ = await self._request_peer(
                endpoint,
                Op.EXISTS,
                Request(key).to_meta(),
            )
            exists = meta.get('exists')
            if not isinstance(exists, bool):
                raise PeerRequestError(
                    f'Peer {endpoint} returned a malformed EXISTS response.',
                )
            return exists
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
            PeeringNotAvailableError: If `endpoint` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: GET key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            status, _, data = await self._request_peer(
                endpoint,
                Op.GET,
                Request(key).to_meta(),
            )
            return None if status == Status.NOT_FOUND else data
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
            PeeringNotAvailableError: If `endpoint` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: SET key=%s on endpoint=%s',
            self._log_prefix,
            key,
            endpoint,
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            await self._request_peer(
                endpoint,
                Op.SET,
                Request(key).to_meta(),
                data,
            )
        else:
            await self._storage.set(key, data)

    async def ping(self, endpoint: EndpointId | None = None) -> PingResult:
        """Measure the latency of and path to a peer endpoint.

        Args:
            endpoint: Peer endpoint to ping. If unspecified, the local endpoint
                is pinged which returns immediately.

        Returns:
            The round-trip time to the peer and the path of the connection.

        Raises:
            PeeringNotAvailableError: If `endpoint` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to the peer endpoint fails.
        """
        if not self._is_peer_request(endpoint):
            return PingResult()
        assert endpoint is not None
        assert self._peer_manager is not None

        start = time.perf_counter()
        await self._request_peer(endpoint, Op.PING, {})
        rtt_ms = (time.perf_counter() - start) * 1000
        path = self._peer_manager.path(endpoint)
        if path is None:  # pragma: no cover
            # The connection closed after the response was received.
            return PingResult(peer_rtt_ms=rtt_ms)
        return PingResult(
            peer_rtt_ms=rtt_ms,
            relayed=path.relayed,
            remote_addr=path.remote_addr,
            path_rtt_ms=path.rtt_ms,
        )

    async def close(self) -> None:
        """Close the endpoint and its peer manager.

        This is idempotent so it is safe to call multiple times.
        """
        if self._closed:
            return
        self._closed = True
        if self._peer_manager is not None:
            await self._peer_manager.close()
        await self._storage.close()
        logger.info('%s: endpoint closed', self._log_prefix)

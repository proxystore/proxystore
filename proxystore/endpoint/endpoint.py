"""Endpoint implementation."""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from types import TracebackType
from typing import Any
from typing import TYPE_CHECKING

from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.handler import handle_request
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import check_response
from proxystore.endpoint.protocol import exists_from_meta
from proxystore.endpoint.protocol import Message
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
    the `target` argument of an operation. See the
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

    def _is_peer_request(self, target: EndpointId | None) -> bool:
        if target is None or target == self.id:
            return False
        if self._peer_manager is None:
            raise PeeringNotAvailableError(
                f'Cannot forward request to endpoint {target} because '
                'peering is not enabled.',
            )
        return True

    async def _request_peer(
        self,
        target: EndpointId,
        op: Op,
        request: Request,
        data: bytes | bytearray | None = None,
    ) -> Message:
        assert self._peer_manager is not None
        logger.debug(
            '%s: sending %s request with key=%s to %s',
            self._log_prefix,
            op.name,
            request.key,
            target,
        )
        response = await self._peer_manager.request(
            target,
            Message(op, request.to_meta(), b'' if data is None else data),
        )
        check_response(
            response,
            op,
            source=f'Peer {target}',
            error=PeerRequestError,
        )
        return response

    async def _handle_peer_request(
        self,
        peer: EndpointId,
        request: Message,
    ) -> Message:
        """Handle a request from a peer endpoint."""
        logger.debug(
            '%s: received op %s request from %s',
            self._log_prefix,
            request.code,
            peer,
        )
        return await handle_request(self, request, forward=False)

    async def evict(
        self,
        key: str,
        target: EndpointId | None = None,
    ) -> None:
        """Evict key from endpoint.

        Args:
            key: Key to evict.
            target: ID of the endpoint to perform the operation on. If
                unspecified, the operation is performed on this endpoint.

        Raises:
            PeeringNotAvailableError: If `target` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: EVICT key=%s on target=%s',
            self._log_prefix,
            key,
            target,
        )
        if self._is_peer_request(target):
            assert target is not None
            await self._request_peer(target, Op.EVICT, Request(key))
        else:
            await self._storage.evict(key)

    async def exists(
        self,
        key: str,
        target: EndpointId | None = None,
    ) -> bool:
        """Check if key exists on endpoint.

        Args:
            key: Key to check.
            target: ID of the endpoint to perform the operation on. If
                unspecified, the operation is performed on this endpoint.

        Returns:
            If the key exists.

        Raises:
            PeeringNotAvailableError: If `target` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: EXISTS key=%s on target=%s',
            self._log_prefix,
            key,
            target,
        )
        if self._is_peer_request(target):
            assert target is not None
            response = await self._request_peer(
                target,
                Op.EXISTS,
                Request(key),
            )
            try:
                return exists_from_meta(response.meta)
            except EndpointProtocolError as e:
                # The peer, not the caller, sent the malformed message.
                raise PeerRequestError(f'Peer {target}: {e}') from e
        return await self._storage.exists(key)

    async def get(
        self,
        key: str,
        target: EndpointId | None = None,
    ) -> bytes | bytearray | None:
        """Get value associated with key on endpoint.

        Args:
            key: Key to get value for.
            target: ID of the endpoint to perform the operation on. If
                unspecified, the operation is performed on this endpoint.

        Returns:
            Value associated with key.

        Raises:
            PeeringNotAvailableError: If `target` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: GET key=%s on target=%s',
            self._log_prefix,
            key,
            target,
        )
        if self._is_peer_request(target):
            assert target is not None
            response = await self._request_peer(target, Op.GET, Request(key))
            if response.code == Status.NOT_FOUND:
                return None
            return response.data
        return await self._storage.get(key, None)

    async def set(
        self,
        key: str,
        data: bytes | bytearray,
        target: EndpointId | None = None,
    ) -> None:
        """Set key with data on endpoint.

        Args:
            key: Key to associate with value.
            data: Value to associate with key.
            target: ID of the endpoint to perform the operation on. If
                unspecified, the operation is performed on this endpoint.

        Raises:
            ObjectSizeExceededError: If the max object size is configured and
                the data exceeds that size.
            PeeringNotAvailableError: If `target` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to a peer endpoint fails.
        """
        logger.debug(
            '%s: SET key=%s on target=%s',
            self._log_prefix,
            key,
            target,
        )
        if self._is_peer_request(target):
            assert target is not None
            await self._request_peer(target, Op.SET, Request(key), data)
        else:
            await self._storage.set(key, data)

    async def ping(self, target: EndpointId | None = None) -> PingResult:
        """Measure the latency of and path to a peer endpoint.

        Args:
            target: ID of the peer endpoint to ping. If unspecified, this
                endpoint is pinged which returns immediately.

        Returns:
            The round-trip time to the peer and the path of the connection.

        Raises:
            PeeringNotAvailableError: If `target` is a different endpoint
                and peering is not enabled.
            PeerError: If the request to the peer endpoint fails.
        """
        if not self._is_peer_request(target):
            return PingResult()
        assert target is not None
        assert self._peer_manager is not None

        start = time.perf_counter()
        await self._request_peer(target, Op.PING, Request())
        rtt_ms = (time.perf_counter() - start) * 1000
        path = self._peer_manager.path(target)
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

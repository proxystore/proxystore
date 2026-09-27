"""Dispatch requests to the storage of an endpoint or to its peers.

Requests from clients (see
[`ClientHandler`][proxystore.endpoint.server.ClientHandler]) and from peer
endpoints (see
[`PeerManager`][proxystore.endpoint.p2p.manager.PeerManager]) are both
handled by a [`Dispatcher`][proxystore.endpoint.dispatch.Dispatcher]. A
request for this endpoint is performed on its storage. A request whose
target is another endpoint is forwarded to that peer unchanged, and the
response of the peer is returned unchanged except that the peer is named in
an error message.

To add an operation, add an [`Op`][proxystore.endpoint.protocol.Op], add
its case to `Dispatcher._handle_local()` in this module, and add a method to
the
[`EndpointClient`][proxystore.endpoint.client.EndpointClient]. Requests
for the operation are forwarded to peers without changes to this module.
"""

from __future__ import annotations

import logging
import time
from typing import assert_never

from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import PeeringDisabledError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.p2p.manager import PeerManager
from proxystore.endpoint.protocol import ErrorInfo
from proxystore.endpoint.protocol import ExistsResult
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import MessageData
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.storage import Storage

logger = logging.getLogger(__name__)


class Dispatcher:
    """Dispatches requests to the storage of an endpoint or to its peers.

    Example:
        ```python
        dispatcher = Dispatcher(endpoint_id, MemoryStorage())
        request = Message(Op.SET, Request(key='key').encode(), b'value')
        response = await dispatcher.handle(request)
        assert response.code == Status.OK
        ```

    Args:
        endpoint_id: ID of the endpoint.
        storage: Storage of the endpoint.
        peer_manager: Peer manager used to forward requests to peers or
            `None` if peering is disabled. The owner of the peer manager
            is responsible for starting it with
            [`handle_peer_request()`][proxystore.endpoint.dispatch.Dispatcher.handle_peer_request]
            as its handler and for closing it.
    """

    def __init__(
        self,
        endpoint_id: EndpointId,
        storage: Storage,
        peer_manager: PeerManager | None = None,
    ) -> None:
        if peer_manager is not None and peer_manager.id != endpoint_id:
            raise ValueError(
                f'The ID of the peer manager ({peer_manager.id}) does not '
                f'match the ID of the endpoint ({endpoint_id}).',
            )
        self._id = endpoint_id
        self._storage = storage
        self._peer_manager = peer_manager

    @property
    def id(self) -> EndpointId:
        """ID of the endpoint."""
        return self._id

    @property
    def storage(self) -> Storage:
        """Storage of the endpoint."""
        return self._storage

    @property
    def peer_manager(self) -> PeerManager | None:
        """Peer manager used to forward requests to peers."""
        return self._peer_manager

    async def handle(self, request: Message) -> Message:
        """Handle a request from a client.

        A request whose target is another endpoint is forwarded to that
        peer.

        Args:
            request: Request message.

        Returns:
            The response message. Errors are returned as responses rather \
            than raised (see
            [`Message.from_error()`][proxystore.endpoint.protocol.Message.from_error]).
        """
        return await self._handle(request, source='client', forward=True)

    async def handle_peer_request(
        self,
        peer_id: EndpointId,
        request: Message,
    ) -> Message:
        """Handle a request from a peer endpoint.

        Requests from peers are handled like requests from clients except
        they are never forwarded to another peer. This is the
        [`RequestHandler`][proxystore.endpoint.p2p.manager.RequestHandler]
        of the peer manager.

        Args:
            peer_id: ID of the peer which sent the request.
            request: Request message.
        """
        source = f'peer {self._peer_name(peer_id)}'
        return await self._handle(request, source=source, forward=False)

    async def _handle(
        self,
        request: Message,
        *,
        source: str,
        forward: bool,
    ) -> Message:
        try:
            op = Op(request.code)
        except ValueError:
            op = None
        # The handshake ops are only valid before requests so they are
        # unknown ops to the dispatcher.
        if op is None or op in (Op.HELLO, Op.AUTH):
            logger.debug(
                'Rejected request with unknown op %s from %s',
                request.code,
                source,
            )
            return Message.error(
                Status.BAD_REQUEST,
                f'unknown op {request.code}',
            )

        try:
            meta = Request.decode(request.meta)
            target = meta.target
            if target is None or target == self.id:
                logger.debug(
                    'Handling %s request from %s (key=%s)',
                    op.name,
                    source,
                    meta.key,
                )
                return await self._handle_local(op, meta, request.data)
            if not forward:
                raise EndpointProtocolError(
                    'requests from peers cannot be forwarded',
                )
            return await self._forward(op, target, meta, request.data)
        except EndpointError as e:
            logger.debug(
                '%s request from %s failed: %s',
                op.name,
                source,
                e,
            )
            return Message.from_error(e)
        except Exception as e:
            logger.exception(
                'Unexpected error handling %s request from %s',
                op.name,
                source,
            )
            return Message.error(Status.ERROR, f'unexpected error: {e!r}')

    async def _handle_local(
        self,
        op: Op,
        request: Request,
        data: MessageData,
    ) -> Message:
        if op == Op.GET:
            result = await self._storage.get(_key(request))
            if result is None:
                return Message(Status.NOT_FOUND)
            return Message(Status.OK, data=result)
        if op == Op.SET:
            await self._storage.set(_key(request), data)
            return Message(Status.OK)
        if op == Op.EXISTS:
            exists = await self._storage.exists(_key(request))
            return Message(Status.OK, ExistsResult(exists=exists).encode())
        if op == Op.EVICT:
            await self._storage.evict(_key(request))
            return Message(Status.OK)
        if op == Op.PING:
            return Message(Status.OK, PingResult().encode())
        if op in (Op.HELLO, Op.AUTH):  # pragma: no cover
            raise AssertionError(f'{op.name} is not a local operation.')
        assert_never(op)  # pragma: no cover

    async def _forward(
        self,
        op: Op,
        target: EndpointId,
        request: Request,
        data: MessageData,
    ) -> Message:
        if self._peer_manager is None:
            raise PeeringDisabledError(
                f'Cannot forward request to endpoint {target} because '
                'peering is disabled.',
            )
        logger.debug(
            'Forwarding %s request from client (key=%s) to peer %s',
            op.name,
            request.key,
            self._peer_name(target),
        )
        # The target is removed so the peer handles the request itself.
        peer_request = Message(op, Request(key=request.key).encode(), data)
        start = time.perf_counter()
        response = await self._peer_manager.request(target, peer_request)
        rtt_ms = (time.perf_counter() - start) * 1000

        if response.code not in (Status.OK, Status.NOT_FOUND):
            error = response.error_message
            logger.debug(
                '%s request forwarded to peer %s failed with status %s: %s',
                op.name,
                self._peer_name(target),
                response.code,
                error,
            )
            meta = ErrorInfo(error=f'Peer {target}: {error}').encode()
            return Message(response.code, meta, response.data)
        if op == Op.PING:
            return Message(Status.OK, self._ping_result(target, rtt_ms))
        return response

    def _peer_name(self, peer_id: EndpointId) -> str:
        if self._peer_manager is None:
            return peer_id.log_name('unknown')
        return self._peer_manager.peer_name(peer_id)

    def _ping_result(self, target: EndpointId, rtt_ms: float) -> bytes:
        assert self._peer_manager is not None
        path = self._peer_manager.path(target)
        if path is None:  # pragma: no cover
            # The connection closed after the response was received.
            return PingResult(peer_rtt_ms=rtt_ms).encode()
        return PingResult(
            peer_rtt_ms=rtt_ms,
            relayed=path.relayed,
            remote_addr=path.remote_addr,
            path_rtt_ms=path.rtt_ms,
        ).encode()


def _key(request: Request) -> str:
    if request.key is None:
        raise EndpointProtocolError('Request requires a key.')
    return request.key

"""Dispatch requests to the storage of an endpoint or to its peers.

Warning:
    This module is an implementation detail of endpoints. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).

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
it to `Dispatcher._LOCAL_OPS` and its case to `Dispatcher._handle_local()` in
this module, and add a method to the
[`EndpointClient`][proxystore.endpoint.client.EndpointClient]. Requests
for the operation are forwarded to peers without changes to this module.
"""

from __future__ import annotations

import logging
import time
from typing import assert_never
from typing import ClassVar

from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.endpoint.exceptions import PeeringDisabledError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.p2p.manager import PeerManager
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
        request = Message(Op.SET, Request('key').to_meta(), b'value')
        response = await dispatcher.handle(request, forward=True)
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

    # Operations performed by an endpoint. The handshake ops are only valid
    # before requests so they are unknown ops to the dispatcher.
    _LOCAL_OPS: ClassVar[frozenset[Op]] = frozenset(
        (Op.GET, Op.SET, Op.EXISTS, Op.EVICT, Op.PING),
    )

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

    async def handle(self, request: Message, *, forward: bool) -> Message:
        """Handle a request.

        Args:
            request: Request message.
            forward: Allow the request to be forwarded to a peer endpoint.
                Requests from clients can be forwarded but requests from
                peers are never forwarded again.

        Returns:
            The response message. Errors are returned as responses rather \
            than raised (see
            [`Message.from_error()`][proxystore.endpoint.protocol.Message.from_error]).
        """
        if request.code not in self._LOCAL_OPS:
            return Message.error(
                Status.BAD_REQUEST,
                f'unknown op {request.code}',
            )
        op = Op(request.code)

        try:
            meta = Request.from_meta(request.meta)
            target = meta.target
            if target is None or target == self.id:
                return await self._handle_local(op, meta, request.data)
            if not forward:
                raise EndpointProtocolError(
                    'requests from peers cannot be forwarded',
                )
            return await self._forward(op, target, meta, request.data)
        except EndpointError as e:
            return Message.from_error(e)
        except Exception as e:
            logger.exception('Unexpected error handling %s request', op.name)
            return Message.error(Status.ERROR, f'unexpected error: {e!r}')

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
        logger.debug(
            'Received op %s request from peer %s',
            request.code,
            peer_id.short(),
        )
        return await self.handle(request, forward=False)

    async def _handle_local(
        self,
        op: Op,
        request: Request,
        data: MessageData,
    ) -> Message:
        match op:
            case Op.GET:
                result = await self._storage.get(_key(request))
                if result is None:
                    return Message(Status.NOT_FOUND)
                return Message(Status.OK, data=result)
            case Op.SET:
                await self._storage.set(_key(request), data)
                return Message(Status.OK)
            case Op.EXISTS:
                exists = await self._storage.exists(_key(request))
                return Message(Status.OK, {'exists': exists})
            case Op.EVICT:
                await self._storage.evict(_key(request))
                return Message(Status.OK)
            case Op.PING:
                return Message(Status.OK, PingResult().to_meta())
            case Op.HELLO | Op.AUTH:  # pragma: no cover
                raise AssertionError(f'{op.name} is not a local operation.')
            case _:  # pragma: no cover
                assert_never(op)

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
            'Forwarding %s request with key=%s to %s',
            op.name,
            request.key,
            target.short(),
        )
        # The target is removed so the peer handles the request itself.
        peer_request = Message(op, Request(request.key).to_meta(), data)
        start = time.perf_counter()
        response = await self._peer_manager.request(target, peer_request)
        rtt_ms = (time.perf_counter() - start) * 1000

        if response.code not in (Status.OK, Status.NOT_FOUND):
            error = response.meta.get('error', 'no error message provided')
            meta = {**response.meta, 'error': f'Peer {target}: {error}'}
            return Message(response.code, meta, response.data)
        if op == Op.PING:
            return Message(Status.OK, self._ping_result(target, rtt_ms))
        return response

    def _ping_result(
        self, target: EndpointId, rtt_ms: float
    ) -> dict[str, object]:
        assert self._peer_manager is not None
        path = self._peer_manager.path(target)
        if path is None:  # pragma: no cover
            # The connection closed after the response was received.
            return PingResult(peer_rtt_ms=rtt_ms).to_meta()
        return PingResult(
            peer_rtt_ms=rtt_ms,
            relayed=path.relayed,
            remote_addr=path.remote_addr,
            path_rtt_ms=path.rtt_ms,
        ).to_meta()


def _key(request: Request) -> str:
    if request.key is None:
        raise EndpointProtocolError('Request requires a key.')
    return request.key

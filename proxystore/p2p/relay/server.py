"""Relay server implementation for facilitating WebRTC peer connections.

The relay server (or signaling server) is a lightweight server accessible by
all peers (e.g., has a public IP address) that facilitates the establishment
of peer WebRTC connections.
"""
from __future__ import annotations

import logging
import sys
from typing import Generic
from typing import TypeVar

try:
    import websockets.client
    import websockets.exceptions
    from websockets.server import WebSocketServerProtocol
except ImportError as e:  # pragma: no cover
    import warnings

    warnings.warn(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
        stacklevel=2,
    )

from proxystore.p2p.relay.authenticate import Authenticator
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import RelayServerError
from proxystore.p2p.relay.exceptions import UnauthorizedError
from proxystore.p2p.relay.manager import Client
from proxystore.p2p.relay.manager import ClientManager
from proxystore.p2p.relay.messages import decode_relay_message
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayMessage
from proxystore.p2p.relay.messages import RelayMessageDecodeError
from proxystore.p2p.relay.messages import RelayMessageEncodeError
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse

logger = logging.getLogger(__name__)
UserT = TypeVar('UserT')


class RelayServer(Generic[UserT]):
    """WebRTC relay server.

    The relay server acts as a public third-party that helps two peers
    (endpoints) establish a peer-to-peer connection during the WebRTC
    peer connection initiation process. The relay server's responsibility
    is just to forward session descriptions between two peers, so the
    server can be relatively lightweight and typically only needs to transfer
    two messages to establish a peer connection, after which the peers no
    longer need the relay server.

    To learn more about the WebRTC peer connection process, check out
    https://webrtc.org/getting-started/peer-connections.

    The relay server is built on websockets and designed to be
    served using [`serve()`][proxystore.p2p.relay.run.serve].

    Args:
        authenticator: Authenticator used to identify users from the opening
            websocket headers.
        max_message_bytes: Optional maximum size of client messages in bytes.
            Clients that send oversized messages will have their connections
            closed. Note that message size is computed using
            [`sys.getsizeof()`][sys.getsizeof] so will also include the
            PyObject overhead.
    """

    def __init__(
        self,
        authenticator: Authenticator[UserT],
        max_message_bytes: int | None = None,
    ) -> None:
        self._authenticator = authenticator
        self._client_manager: ClientManager[UserT] = ClientManager()
        self._max_message_bytes = max_message_bytes

    @property
    def authenticator(self) -> Authenticator[UserT]:
        """User authenticator."""
        return self._authenticator

    @property
    def client_manager(self) -> ClientManager[UserT]:
        """Manager of user clients."""
        return self._client_manager

    async def send(self, client: Client[UserT], message: RelayMessage) -> None:
        """Send message on the socket.

        Note:
            Messages are JSON string encoded using
            [`encode_relay_message()`][proxystore.p2p.relay.messages.encode_relay_message].

        Args:
            client: Client to send message to.
            message: Message to encode and send via the websocket connection
                to the client.
        """
        try:
            message_str = encode_relay_message(message)
        except RelayMessageEncodeError as e:
            logger.error(f'Failed to encode message: {e}')
            return

        try:
            await client.websocket.send(message_str)
        except websockets.exceptions.ConnectionClosed:
            logger.error('Connection closed while attempting to send message')

    async def register(
        self,
        websocket: WebSocketServerProtocol,
        request: RelayRegistrationRequest,
    ) -> None:
        """Register client with relay server.

        Args:
            websocket: Websocket connection with client wanting to register.
            request: Registration request message.

        Raises:
            UnauthorizedError: if the websocket request headers are missing
                the authorization headers.
            ForbiddenError: if Globus authentication fails.
            ForbiddenError: if the requested client UUID is already
                registered by another user.
        """
        try:
            auth_user = self.authenticator.authenticate_user(
                websocket.request_headers,
            )
        except RelayServerError as e:
            logging.warning(
                'Failed to authenticate connection request from '
                f'{websocket.remote_address}. {e.__class__.__name__}: {e}',
            )
            raise

        existing_client = self.client_manager.get_client_by_uuid(request.uuid)
        if existing_client is not None:
            if (
                existing_client.user == auth_user
                and existing_client.websocket != websocket
            ):
                logger.info(
                    f'Previously registered client {request.uuid} attempting '
                    'to reregister on new socket so old socket associated '
                    'with existing registration will be closed',
                )
                await self.unregister(existing_client, False)
            elif existing_client.user != auth_user:
                logger.warning(
                    f'User {auth_user} is attempting to register with a UUID'
                    f' ({request.uuid}) that is owned by a different user.',
                )
                raise ForbiddenError(
                    f'The client UUID {request.uuid} is already registered '
                    'to another user.',
                )

        client = Client(
            name=request.name,
            uuid=request.uuid,
            user=auth_user,
            websocket=websocket,
        )
        self.client_manager.add_client(client)
        logger.info(f'Registered client: {client}')

        await self.send(client, RelayResponse(success=True))

    async def unregister(self, client: Client[UserT], expected: bool) -> None:
        """Unregister the endpoint.

        Args:
            client: Client to unregister.
            expected: If the connection was closed intentionally or due to an
                error.
        """
        reason = 'ok' if expected else 'unexpected'
        logger.info(
            f'Unregistering client {client.uuid} ({client.name}) '
            f'for {reason} reason',
        )
        self.client_manager.remove_client(client)
        await client.websocket.close(code=1000 if expected else 1001)

    async def forward(
        self,
        source_client: Client[UserT],
        request: PeerConnectionRequest,
    ) -> None:
        """Forward peer connection request between two clients.

        If an error is encountered, the relay server replies to the source
        client with an error message set in `message.error`.

        Args:
            source_client: Client making forwarding request.
            request: Peer connection request to forward.
        """
        target_client = self.client_manager.get_client_by_uuid(
            request.peer_uuid,
        )
        if target_client is None:
            logger.warning(
                f'Client {source_client.uuid} ({source_client.name}) '
                'attempting to send message to unknown peer '
                f'{request.peer_uuid}',
            )
            request.error = (
                'Cannot forward peer connection message to peer '
                f'{request.peer_uuid} because this peer is not registered '
                'this relay server.'
            )
            await self.send(source_client, request)
            return

        if source_client.user != target_client.user:
            logger.warning(
                f'Client {source_client.uuid} ({source_client.name}) '
                'attempting to send message to peer '
                f'{request.peer_uuid} owned by another user',
            )
            request.error = (
                f'The requested peer {request.peer_uuid} is owned by a '
                'different user.'
            )
            await self.send(source_client, request)
        else:
            logger.info(
                f'Transmitting message from {source_client.uuid} '
                f'({source_client.name}) to {target_client.uuid} '
                f'({target_client.name})',
            )
            await self.send(target_client, request)

    async def _process_message(
        self,
        websocket: WebSocketServerProtocol,
        message: RelayMessage,
    ) -> None:
        # Dispatches the message to the correct method depending on the type
        if isinstance(message, RelayRegistrationRequest):
            await self.register(websocket, message)
        elif isinstance(message, PeerConnectionRequest):
            client = self.client_manager.get_client_by_websocket(websocket)
            if client is None:
                logger.warning(
                    f'Unregistered client at {websocket.remote_address} '
                    f'and claimed client UUID {message.source_uuid} '
                    'attempting to forward peer request without being '
                    'registered.',
                )
                raise ForbiddenError(
                    'Client has not registered and authenticated with the '
                    'relay server.',
                )
            await self.forward(client, message)
        else:
            raise AssertionError('Unreachable.')

    async def handler(  # noqa: C901
        self,
        websocket: WebSocketServerProtocol,
        uri: str,
    ) -> None:
        """Websocket server message handler.

        The handler will close the connection for the following reasons.

        - An unexpected message type is received (code 4000).
        - The client can not be authenticated (code 4001).
        - The client attempts to access forbidden resources (code 4002).
        - The client sends a message larger than the allowed size (code 4003).

        Args:
            websocket: Websocket message was received on.
            uri: URI message was sent to.
        """
        while True:
            try:
                message_str = await websocket.recv()
            except websockets.exceptions.ConnectionClosedOK:
                client = self.client_manager.get_client_by_websocket(websocket)
                if client is not None:
                    await self.unregister(client, expected=True)
                break
            except websockets.exceptions.ConnectionClosedError:
                client = self.client_manager.get_client_by_websocket(websocket)
                if client is not None:
                    await self.unregister(client, expected=False)
                break

            if (
                self._max_message_bytes is not None
                and sys.getsizeof(message_str) > self._max_message_bytes
            ):
                await websocket.close(
                    4003,
                    reason='Message length exceeds limit.',
                )
                logger.warning(
                    f'Client at {websocket.remote_address} sent message with '
                    f'size {sys.getsizeof(message_str)} bytes which exceeds '
                    f'the max configured size of {self._max_message_bytes} '
                    'bytes. Connection closed with error code 4003',
                )
                break

            try:
                if isinstance(message_str, bytes):
                    raise RelayMessageDecodeError(
                        'Got message as bytes but expected str.',
                    )
                message = decode_relay_message(message_str)
            except RelayMessageDecodeError as e:
                logger.error(
                    'Closing websocket because deserialization error was '
                    'caught on message received from '
                    f'{websocket.remote_address}. {e}',
                )
                await websocket.close(4000, reason='Unknown message type.')
                break

            try:
                await self._process_message(websocket, message)
            except UnauthorizedError as e:
                await websocket.close(
                    code=4001,
                    reason=f'{e.__class__.__name__}: {e}',
                )
            except ForbiddenError as e:
                await websocket.close(
                    code=4002,
                    reason=f'{e.__class__.__name__}: {e}',
                )
            except RelayServerError as e:
                response = RelayResponse(
                    success=False,
                    message=f'{e.__class__.__name__}: {e}',
                    error=True,
                )
                await websocket.send(encode_relay_message(response))

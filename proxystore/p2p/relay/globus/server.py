"""Relay server with Globus Auth for facilitating WebRTC peer connections.

The relay server (or signaling server) is a lightweight server accessible by
all peers (e.g., has a public IP address) that facilitates the establishment
of peer WebRTC connections.
"""
from __future__ import annotations

import logging

import globus_sdk

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

from proxystore.p2p.relay.exceptions import BadRequestError
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import RelayServerError
from proxystore.p2p.relay.exceptions import UnauthorizedError
from proxystore.p2p.relay.globus.manager import Client
from proxystore.p2p.relay.globus.manager import ClientManager
from proxystore.p2p.relay.globus.utils import authenticate_user_with_token
from proxystore.p2p.relay.globus.utils import get_token_from_header
from proxystore.p2p.relay.messages import decode_relay_message
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayMessage
from proxystore.p2p.relay.messages import RelayMessageDecodeError
from proxystore.p2p.relay.messages import RelayMessageEncodeError
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse

logger = logging.getLogger(__name__)


class GlobusAuthRelayServer:
    """WebRTC relay server with Globus Auth.

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
    served using [`serve()`][proxystore.p2p.relay.basic.server.serve].

    Args:
        auth_client: Confidential application authentication client which is
            used for introspecting client tokens.
    """

    def __init__(
        self,
        auth_client: globus_sdk.ConfidentialAppAuthClient,
    ) -> None:
        self._auth_client = auth_client
        self._client_manager = ClientManager()

    @property
    def client_manager(self) -> ClientManager:
        """Manager of user clients."""
        return self._client_manager

    async def send(
        self,
        websocket: WebSocketServerProtocol,
        message: RelayMessage,
    ) -> None:
        """Send message on the socket.

        Args:
            websocket: Websocket to send message on.
            message: Message to json encode and send.
        """
        try:
            message_str = encode_relay_message(message)
        except RelayMessageEncodeError as e:
            logger.error(f'Failed to encode message: {e}')
            return

        try:
            await websocket.send(message_str)
        except websockets.exceptions.ConnectionClosed:
            logger.error('Connection closed while attempting to send message')

    async def register(
        self,
        websocket: WebSocketServerProtocol,
        request: RelayRegistrationRequest,
    ) -> None:
        """Register peer with relay server.

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
        token = get_token_from_header(websocket.request_headers)
        globus_user = authenticate_user_with_token(self._auth_client, token)

        existing_client = self.client_manager.get_client_by_uuid(request.uuid)
        if existing_client is not None:
            if existing_client.globus_user == globus_user:
                logger.info(
                    f'Previously registered client {request.uuid} attempting '
                    'to reregister so old registration will be removed',
                )
                await self.unregister(existing_client.websocket, False)
            else:
                logger.warning(
                    f'User {globus_user} is attempting to register with a UUID'
                    f' ({request.uuid}) that is owned by a different user.',
                )
                raise ForbiddenError(
                    f'The client UUID {request.uuid} is already registered '
                    'to another user.',
                )

        client = Client(
            name=request.name,
            uuid=request.uuid,
            globus_user=globus_user,
            websocket=websocket,
        )

        self.client_manager.add_client(client)
        await self.send(websocket, RelayResponse(success=True))

    async def unregister(
        self,
        websocket: WebSocketServerProtocol,
        expected: bool,
    ) -> None:
        """Unregister the endpoint.

        Args:
            websocket: Websocket connection that was closed.
            expected: If the connection was closed intentionally or due to an
                error.
        """
        client = self.client_manager.get_client_by_websocket(websocket)
        if client is None:
            # Most likely websocket closed before registration was performed
            return

        reason = 'ok' if expected else 'unexpected'
        logger.info(
            f'Unregistering client {client.uuid} ({client.name}) '
            f'for {reason} reason',
        )
        self.client_manager.remove_client(client)
        await client.websocket.close(code=1000 if expected else 1001)

    async def connect(
        self,
        websocket: WebSocketServerProtocol,
        message: PeerConnectionRequest,
    ) -> None:
        """Pass peer connection messages between clients.

        Args:
            websocket: Websocket connection with client that sent the peer
                connection message.
            message: Message to forward to peer client.

        Raises:
            BadRequestError: if the target peer is not registered with this
                relay server.
            ForbiddenError: if the requesting client has not registered.
            ForbiddenError: if the target peer is not owned by the requesting
                user.
        """
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

        peer_client = self.client_manager.get_client_by_uuid(message.peer_uuid)
        if peer_client is None:
            logger.warning(
                f'Client {client.uuid} ({client.name}) attempting to send '
                f'message to unknown peer {message.peer_uuid}',
            )
            raise BadRequestError(
                'Cannot forward peer connection message to peer '
                f'{message.peer_uuid} because this peer is not registered '
                'this relay server.',
            )

        if client.globus_user != peer_client.globus_user:
            logger.warning(
                f'Client {client.uuid} ({client.name}) attempting to send '
                f'message to peer {message.peer_uuid} owned by another user',
            )
            raise ForbiddenError(
                f'The requested peer {message.peer_uuid} is owned by a '
                'different user.',
            )

        logger.info(
            f'Transmitting message from {client.uuid} ({client.name}) '
            f'to {message.peer_uuid}',
        )
        await self.send(peer_client.websocket, message)

    async def handler(
        self,
        websocket: WebSocketServerProtocol,
        uri: str,
    ) -> None:
        """Websocket server message handler.

        The handler will close the connection for the following reasons.
        - An unexpected message type is received (code 4000).
        - The client can not be authenticated (code 4001).
        - The client attempts to access forbidden resources (code 4002).

        Args:
            websocket: Websocket message was received on.
            uri: URI message was sent to.
        """
        while True:
            try:
                message_str = await websocket.recv()
                if isinstance(message_str, str):
                    message = decode_relay_message(message_str)
                else:
                    raise AssertionError(
                        'Received non-str type on websocket.',
                    )
            except websockets.exceptions.ConnectionClosedOK:
                await self.unregister(websocket, expected=True)
                break
            except websockets.exceptions.ConnectionClosedError:
                await self.unregister(websocket, expected=False)
                break
            except RelayMessageDecodeError as e:
                logger.error(
                    'Caught deserialization error on message received from '
                    f'{websocket.remote_address}: {e} ...closing websocket',
                )
                await websocket.close(4000, reason='Unknown message type.')
                break

            try:
                if isinstance(message, RelayRegistrationRequest):
                    await self.register(websocket, message)
                elif isinstance(message, PeerConnectionRequest):
                    await self.connect(websocket, message)
                else:
                    raise AssertionError('Unreachable.')
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
                await self.send(websocket, response)

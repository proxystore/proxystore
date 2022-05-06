"""Signaling server for P2P connections."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import uuid
from dataclasses import dataclass
from socket import gethostname
from typing import Sequence

import websockets
from websockets import WebSocketServerProtocol

from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.exceptions import PeerUnknownError
from proxystore.p2p.messages import BaseMessage
from proxystore.p2p.messages import PeerConnectionMessage
from proxystore.p2p.messages import PeerRegistrationRequest
from proxystore.p2p.messages import PeerRegistrationResponse
from proxystore.p2p.messages import ServerError
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


@dataclass
class Client:
    """Client connection."""

    name: str
    uuid: str
    websocket: WebSocketServerProtocol


class SignalingServer:
    """Signaling Server implementation.

    The Signaling Server acts as a public third-party that helps two peers
    (endpoints) establish a peer-to-peer connection.
    """

    def __init__(self) -> None:
        """Init SignalingServer."""
        self._websocket_to_client: dict[WebSocketServerProtocol, Client] = {}
        self._uuid_to_client: dict[str, Client] = {}

    async def send(
        self,
        websocket: WebSocketServerProtocol,
        message: BaseMessage,
    ) -> None:
        """Send message on the socket.

        Args:
            websocket (WebSocketServerProtocol): websocket to send message on.
            message (BaseMesssage): message to serialize and send.
        """
        message_bytes = serialize(message)
        await websocket.send(message_bytes)

    async def register(
        self,
        websocket: WebSocketServerProtocol,
        request: PeerRegistrationRequest,
    ) -> None:
        """Register peer with Signaling Server.

        Args:
            websocket (WebSocketServerProtocol): websocket connection with
                client wanting to register.
            request (PeerRegistrationRequest): registration request message.
        """
        if request.uuid is None:
            # New client so generate uuid for them
            uuid_ = str(uuid.uuid4())
        else:
            uuid_ = request.uuid

        if websocket not in self._websocket_to_client:
            # Check if previous client reconnected on new socket so unregister
            # old socket. Warning: could be a client impersontating another
            if uuid_ in self._uuid_to_client:
                await self.unregister(
                    self._uuid_to_client[uuid_].websocket,
                    False,
                )
            client = Client(
                name=request.name,
                uuid=uuid_,
                websocket=websocket,
            )
            self._websocket_to_client[websocket] = client
            self._uuid_to_client[client.uuid] = client
            logger.info(
                f'registered {client.uuid} ({client.name} at '
                f'{websocket.remote_address})',
            )
        else:
            client = self._websocket_to_client[websocket]

        await self.send(
            websocket,
            PeerRegistrationResponse(uuid=client.uuid),
        )

    async def unregister(
        self,
        websocket: WebSocketServerProtocol,
        expected: bool,
    ) -> None:
        """Unregister the endpoint.

        Args:
            websocket (WebSocketServerProtocol): websocket connection that
                was closed.
            expected (bool): if the connection was closed intentionally or
                due to an error.
        """
        client = self._websocket_to_client.pop(websocket, None)
        if client is None:
            # Most likely websocket closed before registration was performed
            return
        self._uuid_to_client.pop(client.uuid, None)
        await client.websocket.close(code=1000 if expected else 1001)
        if expected:
            logger.info(
                f'connection closed by {client.uuid} ({client.name})',
            )
        else:
            logger.info(
                f'connection lost from {client.uuid} ({client.name})',
            )

    async def connect(
        self,
        websocket: WebSocketServerProtocol,
        message: PeerConnectionMessage,
    ) -> None:
        """Pass peer connection messages between clients.

        Args:
            websocket (WebSocketServerProtocol): websocket connection with
                client that sent the peer connection message.
            message (PeerConnectionMessage): message to forward to peer client.
        """
        if message.peer_uuid not in self._uuid_to_client:
            await self.send(
                websocket,
                PeerConnectionMessage(
                    source_uuid=self._websocket_to_client[websocket].uuid,
                    source_name=self._websocket_to_client[websocket].name,
                    peer_uuid=message.peer_uuid,
                    error=PeerUnknownError(
                        'peer {message.peer_uuid} is unknown',
                    ),
                ),
            )
            return

        peer_client = self._uuid_to_client[message.peer_uuid]
        logger.info(
            f'transmitting {type(message)} message from {message.source_uuid} '
            f'to {message.peer_uuid}',
        )
        await self.send(peer_client.websocket, message)

    async def handler(
        self,
        websocket: WebSocketServerProtocol,
        uri: str,
    ) -> None:
        """Websocket server message handler.

        Args:
            websocket (WebSocketServerProtocol): websocket message was
                received on.
            uri (str): uri message was sent to.
        """
        while True:
            try:
                message = deserialize(await websocket.recv())
            except websockets.exceptions.ConnectionClosedOK:
                await self.unregister(websocket, expected=True)
                break
            except websockets.exceptions.ConnectionClosedError:
                await self.unregister(websocket, expected=False)
                break
            except SerializationError:
                logger.error(
                    'caught deserialization error on message received from '
                    f'{websocket.remote_address}... skipping message',
                )
            else:
                logger.info(
                    f'received {type(message)} from '
                    f'{websocket.remote_address}',
                )

                if isinstance(message, PeerRegistrationRequest):
                    await self.register(websocket, message)
                elif websocket not in self._websocket_to_client:
                    # If message is not a registration request but this client
                    # has not yet registered, let them know
                    await self.send(
                        websocket,
                        ServerError('client has not registered yet'),
                    )
                elif isinstance(message, PeerConnectionMessage):
                    await self.connect(websocket, message)
                else:
                    await self.send(
                        websocket,
                        ServerError('unknown request type'),
                    )


async def connect(
    address: str,
    uuid: str | None = None,
    name: str | None = None,
    timeout: int = 10,
) -> tuple[str, str, WebSocketServerProtocol]:
    """Establish client connection to a Signaling Server.

    Args:
        address (str): address of the Signaling Server.
        uuid (str, optional): optional uuid of client to use when registering
            with signaling server (default: None).
        name (str, optional): readable name of the client to use when
            registering with the signaling server. By default the
            hostname will be used (default: None).
        timeout (int): time to wait in seconds on server connections
            (default: 10).

    Returns:
        tuple of the UUID of this client returned by the signaling server,
        the name used to register the client, and the websocket connection to
        the signaling server.

    Raises:
        EndpointRegistrationError:
            if the connection to the signaling server is closed, does not reply
            to the registration request within the timeout, or replies with an
            error.
    """
    if name is None:
        name = gethostname()
    websocket = await websockets.connect(
        f'ws://{address}',
        open_timeout=timeout,
    )
    await websocket.send(
        serialize(PeerRegistrationRequest(uuid=uuid, name=name)),
    )
    try:
        message = deserialize(
            await asyncio.wait_for(websocket.recv(), timeout),
        )
    except websockets.exceptions.ConnectionClosed:
        raise PeerRegistrationError(
            'Connection to signaling server closed before peer '
            'registration completed.',
        )
    except asyncio.TimeoutError:
        raise PeerRegistrationError(
            'Signaling server did not reply to registration within timeout.',
        )

    if isinstance(message, PeerRegistrationResponse):
        if message.error is not None:
            raise PeerRegistrationError(
                'Failed to register as peer with signaling server. '
                f'Got exception: {message.error}',
            )
        return message.uuid, name, websocket
    else:
        raise PeerRegistrationError(
            'Signaling server replied with unknown message type: '
            f'{type(message)}.',
        )


async def serve(host: str, port: int) -> None:
    """Run the signaling server.

    Initializes a :class:`SignalingServer <.SignalingServer>` and starts a
    websocket server listening on `host:port` for new connections and
    incoming messages.

    Args:
        host (str): host to listen on.
        port (int): port to listen on.
    """
    server = SignalingServer()
    # Set the stop condition when receiving SIGTERM (ctrl-C).
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    async with websockets.serve(server.handler, host, port):
        await stop


def main(argv: Sequence[str] | None = None) -> int:
    """CLI for starting the signaling server."""
    parser = argparse.ArgumentParser('Websocket-based Signaling Server')
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='host to listen on (defaults to 0.0.0.0 for all addresses)',
    )
    parser.add_argument(
        '--port',
        default=8765,
        type=int,
        help='port to listen on',
    )
    parser.add_argument(
        '--log-level',
        choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO',
        help='logging level',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level)

    asyncio.run(serve(args.host, args.port))

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

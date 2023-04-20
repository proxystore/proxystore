"""Relay server implementation for WebRTC peer connections.

The relay server (or signaling server) is a lightweight server accessible by
all peers (e.g., has a public IP address) that facilitates the establishment
of peer WebRTC connections.
"""
from __future__ import annotations

import asyncio
import datetime
import logging.handlers
import os
import signal
import ssl
import sys
from dataclasses import dataclass
from uuid import UUID

import click

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

from proxystore.p2p import messages

logger = logging.getLogger(__name__)


@dataclass
class Client:
    """Representation of client connection.

    Attributes:
        name: Name of client.
        uuid: UUID of client.
        websocket: WebSocket connection to the client.
    """

    name: str
    uuid: UUID
    websocket: WebSocketServerProtocol


class RelayServer:
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
    served using [`websockets.serve()`][websockets.server.serve].

    Example:
        ```python
        import websockets
        from proxystore.p2p.relay import RelayServer

        relay_server = RelayServer()
        async with websockets.serve(
             relay_server.handler, host='localhost', port=1234
        ) as websocket_server:
            ...
        ```
    """

    def __init__(self) -> None:
        self._websocket_to_client: dict[WebSocketServerProtocol, Client] = {}
        self._uuid_to_client: dict[UUID, Client] = {}

    async def send(
        self,
        websocket: WebSocketServerProtocol,
        message: messages.Message,
    ) -> None:
        """Send message on the socket.

        Args:
            websocket: Websocket to send message on.
            message: Message to json encode and send.
        """
        try:
            message_str = messages.encode(message)
        except messages.MessageEncodeError as e:
            logger.error(f'Failed to encode message: {e}')
            return

        try:
            await websocket.send(message_str)
        except websockets.exceptions.ConnectionClosed:
            logger.error('Connection closed while attempting to send message')

    async def register(
        self,
        websocket: WebSocketServerProtocol,
        request: messages.ServerRegistration,
    ) -> None:
        """Register peer with relay server.

        Args:
            websocket: Websocket connection with client wanting to register.
            request: Registration request message.
        """
        if websocket not in self._websocket_to_client:
            # Check if previous client reconnected on new socket so unregister
            # old socket. Warning: could be a client impersontating another
            if request.uuid in self._uuid_to_client:
                logger.info(
                    f'Previously registered client {request.uuid} attempting '
                    'to reregister so old registration will be removed',
                )
                await self.unregister(
                    self._uuid_to_client[request.uuid].websocket,
                    False,
                )
            client = Client(
                name=request.name,
                uuid=request.uuid,
                websocket=websocket,
            )
            self._websocket_to_client[websocket] = client
            self._uuid_to_client[client.uuid] = client
            logger.info(
                f'Registered {client.uuid} ({client.name} at '
                f'{websocket.remote_address})',
            )
        else:
            client = self._websocket_to_client[websocket]
            logger.info(
                f'Previously registered client {client.uuid} attempting to '
                'reregister so previous registration will be returned',
            )

        await self.send(websocket, messages.ServerResponse(success=True))

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
        client = self._websocket_to_client.pop(websocket, None)
        if client is None:
            # Most likely websocket closed before registration was performed
            return
        reason = 'ok' if expected else 'unexpected'
        logger.info(
            f'Unregistering client {client.uuid} ({client.name}) '
            f'for {reason} reason',
        )
        self._uuid_to_client.pop(client.uuid, None)
        await client.websocket.close(code=1000 if expected else 1001)

    async def connect(
        self,
        websocket: WebSocketServerProtocol,
        message: messages.PeerConnection,
    ) -> None:
        """Pass peer connection messages between clients.

        Args:
            websocket: Websocket connection with client that sent the peer
                connection message.
            message: Message to forward to peer client.
        """
        client = self._websocket_to_client[websocket]
        if message.peer_uuid not in self._uuid_to_client:
            logger.warning(
                f'Client {client.uuid} ({client.name}) attempting to send '
                f'message to unknown peer {message.peer_uuid}',
            )
            message.error = (
                'Cannot forward peer connection message to peer '
                f'{message.peer_uuid} because this peer is unknown.'
            )
            await self.send(websocket, message)
        else:
            peer_client = self._uuid_to_client[message.peer_uuid]
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

        Args:
            websocket: Websocket message was received on.
            uri: URI message was sent to.
        """
        logger.info('Relay server listening for incoming connections')
        while True:
            try:
                message_str = await websocket.recv()
                if isinstance(message_str, str):
                    message = messages.decode(message_str)
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
            except messages.MessageDecodeError as e:
                logger.error(
                    'Caught deserialization error on message received from '
                    f'{websocket.remote_address}: {e} ...skipping message',
                )
                continue

            if isinstance(message, messages.ServerRegistration):
                await self.register(websocket, message)
            elif isinstance(message, messages.PeerConnection):
                if websocket in self._websocket_to_client:
                    await self.connect(websocket, message)
                else:
                    # If message is not a registration request but this client
                    # has not yet registered, let them know
                    logger.info(
                        'Returning server error to message received from '
                        f'unregistered client {message.source_uuid} '
                        f'({message.source_name})',
                    )
                    response = messages.ServerResponse(
                        success=False,
                        message='client has not registered yet',
                        error=True,
                    )
                    await self.send(websocket, response)
            else:
                raise AssertionError('Unreachable.')


async def serve(
    host: str,
    port: int,
    certfile: str | None = None,
    keyfile: str | None = None,
) -> None:
    """Run the relay server.

    Initializes a [`RelayServer`][proxystore.p2p.relay.RelayServer]
    and starts a websocket server listening on `host:port` for new connections
    and incoming messages.

    Args:
        host: Host to listen on.
        port: Port to listen on.
        certfile: Optional certificate file (PEM format) to enable TLS while
            serving.
        keyfile: Optional private key file. If not specified, the key will be
            taken from the certfile.
    """
    server = RelayServer()

    # Set the stop condition when receiving SIGINT (ctrl-C) and SIGTERM.
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    ssl_context: ssl.SSLContext | None = None
    if certfile is not None:  # pragma: no cover
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(certfile, keyfile=keyfile)

    async with websockets.server.serve(
        server.handler,
        host,
        port,
        logger=logger,
        ssl=ssl_context,
    ):
        logger.info(f'Serving relay server on {host}:{port}')
        logger.info('Use ctrl-C to stop')
        await stop

    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)

    logger.info('Server closed')


@click.command()
@click.option(
    '--host',
    default='0.0.0.0',
    metavar='ADDR',
    help='Address to listen on.',
)
@click.option(
    '--port',
    default=8765,
    type=int,
    metavar='PORT',
    help='Port to listen on.',
)
@click.option(
    '--certfile',
    default=None,
    metavar='PATH',
    help='Certificate file for serving with TLS.',
)
@click.option(
    '--keyfile',
    default=None,
    metavar='PATH',
    help='Private key file associated with the certfile.',
)
@click.option(
    '--log-dir',
    default=None,
    metavar='PATH',
    help='Write server logs to this directory.',
)
@click.option(
    '--log-level',
    default='INFO',
    type=click.Choice(
        ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
        case_sensitive=False,
    ),
    help='Minimum logging level.',
)
def cli(
    host: str,
    port: int,
    certfile: str | None,
    keyfile: str | None,
    log_dir: str | None,
    log_level: str,
) -> None:
    """Run a relay server instance.

    The relay server is used by clients to establish peer-to-peer
    WebRTC connections.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_dir is not None:
        os.makedirs(log_dir, exist_ok=True)
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(
                os.path.join(log_dir, 'server.log'),
                # Rotate logs Sunday at midnight
                when='W6',
                atTime=datetime.time(hour=0, minute=0, second=0),
            ),
        )

    logging.basicConfig(
        format=(
            '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S',
        level=log_level,
        handlers=handlers,
    )

    asyncio.run(serve(host, port, certfile=certfile, keyfile=keyfile))

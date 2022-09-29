"""Signaling server implementation for WebRTC peer connections."""
from __future__ import annotations

import argparse
import asyncio
import datetime
import logging.handlers
import os
import signal
import ssl
import sys
from dataclasses import dataclass
from typing import Sequence
from uuid import UUID

try:
    import websockets.client
    import websockets.exceptions
    from websockets.server import WebSocketServerProtocol
except ImportError as e:  # pragma: no cover
    import warnings

    warnings.warn(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
    )

from proxystore.p2p import messages

logger = logging.getLogger(__name__)


@dataclass
class Client:
    """Representation of client connection."""

    name: str
    uuid: UUID
    websocket: WebSocketServerProtocol


class SignalingServer:
    """Signaling Server implementation.

    The Signaling Server acts as a public third-party that helps two peers
    (endpoints) establish a peer-to-peer connection during the WebRTC
    peer connection initiation process. The signaling server's responsibility
    is just to forward session descriptions between two peers, so the
    server can be relatively lightweight and typically only needs to transfer
    two messages to establish a peer connection, after which the peers no
    longer need the signaling server.

    To learn more about the WebRTC peer connection process, check out
    `<https://webrtc.org/getting-started/peer-connections>`_.

    The signaling server is built on websockets and designed to be
    served using :code:`websockets.serve`.

    .. code-block:: python

       import websockets
       from proxystore.p2p.server import SignalingServer

       signaling_server = SignalingServer()
       async with websockets.serve(
            signaling_server.handler, host='localhost', port=1234
       ) as websocket_server:
           ...
    """

    def __init__(self) -> None:
        """Init SignalingServer."""
        self._websocket_to_client: dict[WebSocketServerProtocol, Client] = {}
        self._uuid_to_client: dict[UUID, Client] = {}

    async def send(
        self,
        websocket: WebSocketServerProtocol,
        message: messages.Message,
    ) -> None:
        """Send message on the socket.

        Args:
            websocket (WebSocketServerProtocol): websocket to send message on.
            message (Message): message to json encode and send.
        """
        try:
            message_str = messages.encode(message)
        except messages.MessageEncodeError as e:
            logger.error(f'failed to encode message: {e}')
            return

        try:
            await websocket.send(message_str)
        except websockets.exceptions.ConnectionClosed:
            logger.error('connection closed while attempting to send message')

    async def register(
        self,
        websocket: WebSocketServerProtocol,
        request: messages.ServerRegistration,
    ) -> None:
        """Register peer with Signaling Server.

        Args:
            websocket (WebSocketServerProtocol): websocket connection with
                client wanting to register.
            request (ServerRegistration): registration request message.
        """
        if websocket not in self._websocket_to_client:
            # Check if previous client reconnected on new socket so unregister
            # old socket. Warning: could be a client impersontating another
            if request.uuid in self._uuid_to_client:
                logger.info(
                    f'previously registered client {request.uuid} attempting '
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
                f'registered {client.uuid} ({client.name} at '
                f'{websocket.remote_address})',
            )
        else:
            client = self._websocket_to_client[websocket]
            logger.info(
                f'previously registered client {client.uuid} attempting to '
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
            websocket (WebSocketServerProtocol): websocket connection that
                was closed.
            expected (bool): if the connection was closed intentionally or
                due to an error.
        """
        client = self._websocket_to_client.pop(websocket, None)
        if client is None:
            # Most likely websocket closed before registration was performed
            return
        reason = 'ok' if expected else 'unexpected'
        logger.info(
            f'unregistering client {client.uuid} ({client.name}) '
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
            websocket (WebSocketServerProtocol): websocket connection with
                client that sent the peer connection message.
            message (PeerConnectionMessage): message to forward to peer client.
        """
        client = self._websocket_to_client[websocket]
        if message.peer_uuid not in self._uuid_to_client:
            logger.warning(
                f'client {client.uuid} ({client.name}) attempting to send '
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
                f'transmitting message from {client.uuid} ({client.name}) '
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
        logger.info('signaling server listening for incoming connections')
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
                    'caught deserialization error on message received from '
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
                        'returning server error to message received from '
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
    """Run the signaling server.

    Initializes a :class:`SignalingServer <.SignalingServer>` and starts a
    websocket server listening on `host:port` for new connections and
    incoming messages.

    Args:
        host (str): host to listen on.
        port (int): port to listen on.
        certfile (str): optional certificate file (PEM format) to enable
            TLS while serving.
        keyfile (str): optional private key file. If not specified, the key
            will be taken from the certfile.
    """
    server = SignalingServer()

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
        logger.info(f'serving signaling server on {host}:{port}')
        logger.info('use ctrl-C to stop')
        await stop

    logger.info('server closed')


def main(argv: Sequence[str] | None = None) -> int:
    """CLI for starting the signaling server.

    Usage:

    .. code-block:: console

       $ signaling-server {options}
       $ signaling-server --help
    """
    parser = argparse.ArgumentParser(
        'Websocket-based Signaling Server',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='host to listen on',
    )
    parser.add_argument(
        '--port',
        default=8765,
        type=int,
        help='port to listen on',
    )
    parser.add_argument(
        '--certfile',
        default=None,
        help='certificate file for serving with TLS',
    )
    parser.add_argument(
        '--keyfile',
        default=None,
        help='private key file associated with the certificate file',
    )
    parser.add_argument(
        '--log-dir',
        default=None,
        help='write logs named server.log.{timestamp} to this dir',
    )
    parser.add_argument(
        '--log-level',
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO',
        help='logging level',
    )
    args = parser.parse_args(argv)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_dir is not None:
        os.makedirs(args.log_dir, exist_ok=True)
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(
                os.path.join(args.log_dir, 'server.log'),
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
        level=args.log_level,
        handlers=handlers,
    )

    asyncio.run(
        serve(
            args.host,
            args.port,
            certfile=args.certfile,
            keyfile=args.keyfile,
        ),
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

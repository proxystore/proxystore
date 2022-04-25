"""Signaling server for P2P connections."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass
from typing import Sequence

import websockets
from websockets import WebSocketServerProtocol

from proxystore.blobspace.exceptions import EndpointNotRegisteredError
from proxystore.blobspace.exceptions import EndpointRegistrationError
from proxystore.blobspace.exceptions import ServerException
from proxystore.blobspace.exceptions import UnknownMessageType
from proxystore.blobspace.messages import BaseMessage
from proxystore.blobspace.messages import EndpointRegistrationRequest
from proxystore.blobspace.messages import EndpointRegistrationSuccess
from proxystore.blobspace.messages import P2PConnectionBaseMessage
from proxystore.blobspace.messages import P2PConnectionError
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
        message: BaseMessage | ServerException,
    ) -> None:
        """Send message on the socket.

        Args:
            websocket (WebSocketServerProtocol): websocket to send message on.
            message (BaseMesssage, ServerException): message to serialize and
                send.
        """
        message_bytes = serialize(message)
        await websocket.send(message_bytes)

    async def register(
        self,
        websocket: WebSocketServerProtocol,
        request: EndpointRegistrationRequest,
    ) -> None:
        """Register endpoint with Signaling Server.

        Args:
            websocket (WebSocketServerProtocol): websocket connection with
                client wanting to register.
            request (EndpointRegistrationRequest): request message from the
                client.
        """
        if websocket not in self._websocket_to_client:
            client = Client(request.name, request.uuid, websocket)
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
            EndpointRegistrationSuccess(name=client.name, uuid=client.uuid),
        )

    def unregister(
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
        if expected:
            logger.info(f'connection closed by {client.uuid} ({client.name})')
        else:
            logger.info(f'connection lost from {client.uuid} ({client.name})')

    async def connect(
        self,
        websocket: WebSocketServerProtocol,
        message: P2PConnectionBaseMessage,
    ) -> None:
        """Pass P2P connection messages between clients.

        Args:
            websocket (WebSocketServerProtocol): websocket connection with
                client that sent the P2P connection message.
            message (P2PConnectionMessage): message to forward to target
                client.
        """
        if message.target_uuid not in self._uuid_to_client:
            await self.send(
                websocket,
                P2PConnectionError(
                    source_uuid=message.source_uuid,
                    target_uuid=message.target_uuid,
                    error=f'target {message.target_uuid} is unknown',
                ),
            )
            return

        target_client = self._uuid_to_client[message.target_uuid]
        logger.info(
            f'transmitting {type(message)} message from {message.source_uuid} '
            f'to {message.target_uuid}',
        )
        await self.send(target_client.websocket, message)

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
                self.unregister(websocket, expected=True)
                break
            except websockets.exceptions.ConnectionClosedError:
                self.unregister(websocket, expected=False)
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

                if isinstance(message, EndpointRegistrationRequest):
                    await self.register(websocket, message)
                elif websocket not in self._websocket_to_client:
                    # If message is not a registration request but this client
                    # has not yet registered, let them know
                    await self.send(websocket, EndpointNotRegisteredError())
                elif isinstance(message, P2PConnectionBaseMessage):
                    await self.connect(websocket, message)
                else:
                    await self.send(websocket, UnknownMessageType())


async def connect(
    uuid: str,
    name: str,
    address: str,
    timeout: int = 10,
) -> WebSocketServerProtocol:
    """Establish client connection to a Signaling Server.

    Args:
        uuid (str): unique uuid of the client.
        name (str): readable name of client.
        address (str): address of the Signaling Server.
        timeout (int): time to wait in seconds on server connections.

    Returns:
        websocket connection to the signaling server.

    Raises:
        EndpointRegistrationError:
            if the connection to the signaling server is closed, does not reply
            to the registration request within the timeout, replies with
            mismatched endpoint UUID or name, or does not reply with an
            EndpointRegistrationSuccess message.
    """
    websocket = await websockets.connect(
        f'ws://{address}',
        open_timeout=timeout,
    )
    await websocket.send(
        serialize(EndpointRegistrationRequest(name=name, uuid=uuid)),
    )
    try:
        message = deserialize(
            await asyncio.wait_for(websocket.recv(), timeout),
        )
    except websockets.exceptions.ConnectionClosed:
        raise EndpointRegistrationError(
            'connection to signaling server closed before '
            'registration completed',
        )
    except asyncio.TimeoutError:
        raise EndpointRegistrationError(
            'signaling server did not reply to registration within timeout',
        )

    if isinstance(message, EndpointRegistrationSuccess):
        if uuid != message.uuid or name != message.name:
            raise EndpointRegistrationError(
                f'received {type(EndpointRegistrationSuccess)} with '
                f'mismatched data. Expected uuid={uuid} and name={name} but '
                f'got uuid={message.uuid} and name={message.name}',
            )
        return websocket
    else:
        raise EndpointRegistrationError(
            f'did not receive {type(EndpointRegistrationSuccess)} '
            f'confirmation from signaling server at {address}',
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

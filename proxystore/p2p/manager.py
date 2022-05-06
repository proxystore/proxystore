"""Manager of many peer-to-peer connections."""
from __future__ import annotations

import asyncio
import logging
import socket
from types import TracebackType
from typing import Any
from typing import Generator
from uuid import UUID

import websockets
from websockets import WebSocketServerProtocol

from proxystore.p2p.connection import PeerConnection
from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.messages import PeerConnectionMessage
from proxystore.p2p.server import connect
from proxystore.p2p.task import spawn_guarded_background_task
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize

logger = logging.getLogger(__name__)


class PeerManager:
    """Peer Connections Manager.

    Manages individual connections to multiple peers.

    Note:
        The :class:`PeerManager <.PeerManager>` can be
        used as a context manager.

        >>> async with PeerManager(..) as manager:
        >>>     ...
    """

    def __init__(
        self,
        uuid: str | UUID,
        signaling_server: str,
        name: str | None = None,
    ) -> None:
        """Init PeerManager.

        Args:
            uuid (str, UUID): uuid of the client.
            signaling_server (str): address of signaling server to use for
                establishing peer-to-peer connections.
            name (str, optional): readable name of the client to use in
                logging. If unspecified, the hostname will be used.
        """
        self._uuid = uuid if isinstance(uuid, str) else str(uuid)
        self._signaling_server = signaling_server
        self._name = name if name is not None else socket.gethostname()

        self._peers: dict[frozenset[str], PeerConnection] = {}
        self._message_queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._tasks: list[asyncio.Task[None]] = []
        self._websocket_or_none: WebSocketServerProtocol | None = None

        logger.info(
            f'peer manager for {uuid} initialized',
        )

    @property
    def _websocket(self) -> WebSocketServerProtocol:
        if self._websocket_or_none is not None:
            return self._websocket_or_none
        raise RuntimeError(
            f'{self.__class__.__name__} has not established a connection '
            'to the signaling server because async_init() has not been '
            'called yet. Is the manager being initialized with await?',
        )

    @property
    def uuid(self) -> str:
        """Get UUID of the peer manager."""
        return self._uuid

    @property
    def name(self) -> str:
        """Get name of the peer manager."""
        return self._name

    async def async_init(self) -> None:
        """Connect to signaling server."""
        if self._websocket_or_none is None:
            uuid, _, socket = await connect(
                address=self._signaling_server,
                uuid=self._uuid,
                name=self._name,
            )
            if uuid != self._uuid:
                raise PeerRegistrationError(
                    'Signaling server responded to registration request '
                    f'with non-matching UUID. Received {uuid} but expected '
                    f'{self._uuid}.',
                )
            self._websocket_or_none = socket
        if len(self._tasks) == 0:
            self._tasks = [
                spawn_guarded_background_task(self._handle_server_messages),
            ]

    async def __aenter__(self) -> PeerManager:
        """Enter async context manager."""
        await self.async_init()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Leave async context manager and close manager."""
        await self.close()

    def __await__(self) -> Generator[Any, None, PeerManager]:
        """Awaitable constructor."""
        return self.__aenter__().__await__()

    async def _handle_peer_messages(
        self,
        peer_uuid: str,
        connection: PeerConnection,
    ) -> None:
        while True:
            message = deserialize(await connection.recv())
            await self._message_queue.put((peer_uuid, message))

    async def _handle_server_messages(self) -> None:
        """Handle messages from the signaling server.

        Forwards the message to the correct P2PConnection instance.
        """
        while True:
            try:
                message = deserialize(await self._websocket.recv())
            except websockets.exceptions.ConnectionClosedOK:
                break
            except websockets.exceptions.ConnectionClosedError:
                break
            except SerializationError:
                logger.error(
                    'caught deserialization error on message received from '
                    'signaling server... skipping message',
                )
                continue

            logger.info(f'received {type(message)} from signaling server')

            if isinstance(message, PeerConnectionMessage):
                peers = frozenset({message.source_uuid, message.peer_uuid})
                if peers not in self._peers:
                    connection = PeerConnection(
                        uuid=self._uuid,
                        name=self._name,
                        websocket=self._websocket,
                    )
                    self._peers[peers] = connection
                    self._tasks.append(
                        spawn_guarded_background_task(
                            self._handle_peer_messages,
                            message.source_uuid,
                            connection,
                        ),
                    )
                await self._peers[peers].handle_server_message(message)
            else:
                logger.error(
                    'received unknown message type from signaling server: '
                    f'{message}',
                )

    async def close(self) -> None:
        """Close the connection manager.

        Warning:
            Does not close the websocket to the signaling server.
        """
        for connection in self._peers.values():
            await connection.close()
        for task in self._tasks:
            task.cancel()
        if self._websocket_or_none is not None:
            await self._websocket_or_none.close()
        logger.info(f'peer manager for {self._uuid} closed')

    async def recv(self) -> tuple[str, Any]:
        """Receive next message from a peer.

        Returns:
            tuple of endpoint UUID that sent message and the message itself.
        """
        return await self._message_queue.get()

    async def send(self, peer_uuid: str, message: Any) -> None:
        """Send message to peer.

        Args:
            peer_uuid (str): UUID of peer to send message to.
            message (Any): message to send to peer.
        """
        connection = await self.get_connection(peer_uuid)
        await connection.send(serialize(message))

    async def get_connection(self, peer_uuid: str | UUID) -> PeerConnection:
        """Get connection to the peer.

        Args:
            peer_uuid (str, UUID): uuid of peer to make connection with.

        Returns:
            :any:`PeerConnection <PeerConnection>`
        """
        peer_uuid = peer_uuid if isinstance(peer_uuid, str) else str(peer_uuid)
        peers = frozenset({self._uuid, peer_uuid})

        if peers in self._peers:
            return self._peers[peers]

        connection = PeerConnection(self._uuid, self._name, self._websocket)
        await connection.send_offer(peer_uuid)
        self._peers[peers] = connection
        self._tasks.append(
            spawn_guarded_background_task(
                self._handle_peer_messages,
                peer_uuid,
                connection,
            ),
        )
        return connection

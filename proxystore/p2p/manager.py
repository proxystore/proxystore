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
from websockets.client import WebSocketClientProtocol

from proxystore.p2p import messages
from proxystore.p2p.connection import log_name
from proxystore.p2p.connection import PeerConnection
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.server import connect
from proxystore.p2p.task import SafeTaskExit
from proxystore.p2p.task import spawn_guarded_background_task

logger = logging.getLogger(__name__)


class PeerManager:
    """Peer Connections Manager.

    Handles establishing peer connections via
    `aiortc <https://aiortc.readthedocs.io/>`_, responding to requests for
    new peer connections from the signaling server, and sending and
    receiving data to/from existing peer connections.

    .. code-block:: python

       from proxystore.p2p.manager import PeerManager

       pm1 = await PeerManager(uuid.uuid4(), signaling_server.address)
       pm2 = await PeerManager(uuid.uuid4(), signaling_server.address)

       await pm1.send(pm2.uuid, 'hello hello')
       source_uuid, message = await pm2.recv()
       assert source_uuid == pm1.uuid
       assert message == 'hello hello'

       pm1.close()
       pm2.close()

    Note:
        The :class:`PeerManager <.PeerManager>` can also be used as a context
        manager.

        >>> async with PeerManager(..) as manager:
        >>>     ...
    """

    def __init__(
        self,
        uuid: UUID,
        signaling_server: str,
        name: str | None = None,
        timeout: int = 30,
    ) -> None:
        """Init PeerManager.

        Warning:
            The :class:`PeerManager <.PeerManager>` must be initialized
            with await or inside an async with statement to correctly
            configure all async tasks and connections.

            >>> manager = await PeerManager(...)
            >>> manager.close()
            >>>
            >>> async with PeerManager(...) as manager:
            >>>     ...

        Args:
            uuid (str, UUID): uuid of the client.
            signaling_server (str): address of signaling server to use for
                establishing peer-to-peer connections.
            name (str, optional): readable name of the client to use in
                logging. If unspecified, the hostname will be used.
            timeout (int): timeout in seconds when waiting for a peer
                or signaling server connection to be established (default: 30).
        """
        self._uuid = uuid
        self._signaling_server = signaling_server
        self._name = name if name is not None else socket.gethostname()
        self._timeout = timeout

        self._peers_lock = asyncio.Lock()
        self._peers: dict[frozenset[UUID], PeerConnection] = {}
        self._message_queue: asyncio.Queue[
            messages.PeerMessage
        ] = asyncio.Queue()
        self._server_task: asyncio.Task[None] | None = None
        self._tasks: dict[frozenset[UUID], asyncio.Task[None]] = {}
        self._websocket_or_none: WebSocketClientProtocol | None = None

    @property
    def _log_prefix(self) -> str:
        return f'{self.__class__.__name__}[{log_name(self._uuid, self._name)}]'

    @property
    def _websocket(self) -> WebSocketClientProtocol:
        if self._websocket_or_none is not None:
            return self._websocket_or_none
        raise RuntimeError(
            f'{self.__class__.__name__} has not established a connection '
            'to the signaling server because async_init() has not been '
            'called yet. Is the manager being initialized with await?',
        )

    @property
    def uuid(self) -> UUID:
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
                timeout=self._timeout,
            )
            if uuid != self._uuid:
                raise PeerRegistrationError(
                    'Signaling server responded to registration request '
                    f'with non-matching UUID. Received {uuid} but expected '
                    f'{self._uuid}.',
                )
            self._websocket_or_none = socket
            logger.info(
                f'{self._log_prefix}: registered as peer with signaling '
                f'server at {self._signaling_server}',
            )
        if self._server_task is None:
            self._server_task = spawn_guarded_background_task(
                self._handle_server_messages,
            )

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

    async def _check_connection(
        self,
        peer_uuid: UUID,
        connection: PeerConnection,
    ) -> None:
        """Wait on connection to be ready and handle errors.

        If an error is raised, catch it and remove this PeerConnection.

        Warning:
            This method will cancel the task that is handling the peer
            messages.
        """
        try:
            await connection.ready(timeout=self._timeout)
        except PeerConnectionError as e:  # pragma: >=3.8 cover
            logger.error(str(e))
            await connection.close()
            peers = frozenset({self._uuid, peer_uuid})
            async with self._peers_lock:
                self._peers.pop(peers, None)
            raise SafeTaskExit()

    async def _handle_peer_messages(
        self,
        peer_uuid: UUID,
        connection: PeerConnection,
    ) -> None:
        await self._check_connection(peer_uuid, connection)
        assert connection._peer_name is not None
        peer_name = log_name(peer_uuid, connection._peer_name)
        logger.info(
            f'{self._log_prefix}: listening for messages from peer '
            f'{peer_name}',
        )
        while True:
            message_str = await connection.recv()
            if isinstance(message_str, str):
                message = messages.decode(message_str)
            else:
                raise AssertionError(
                    'Received non-str object on peer connection.',
                )
            if isinstance(message, messages.PeerMessage):
                await self._message_queue.put(message)
                logger.debug(
                    f'{self._log_prefix}: placed message from {peer_name} on '
                    'queue',
                )
            else:
                logger.error(
                    f'{self._log_prefix}: received non-peer message type from '
                    f'{peer_name}. Got type {type(message)}',
                )

    async def _handle_server_messages(self) -> None:
        """Handle messages from the signaling server.

        Forwards the message to the correct P2PConnection instance.
        """
        logger.info(
            f'{self._log_prefix}: listening for messages from signaling '
            'server',
        )
        while True:
            try:
                message_str = await self._websocket.recv()
                if isinstance(message_str, str):
                    message = messages.decode(message_str)
                else:
                    raise AssertionError('Received non-str on websocket.')
            except websockets.exceptions.ConnectionClosedOK:
                break
            except websockets.exceptions.ConnectionClosedError:
                break
            except messages.MessageDecodeError as e:
                logger.error(
                    f'{self._log_prefix}: error deserializing message from '
                    f'signaling server: {e} ...skipping message',
                )
                continue

            if isinstance(message, messages.PeerConnection):
                logger.debug(
                    f'{self._log_prefix}: signaling server forwarded peer '
                    'connection message from '
                    f'{log_name(message.source_uuid, message.source_name)}',
                )
                peers = frozenset({message.source_uuid, message.peer_uuid})
                if peers not in self._peers:
                    connection = PeerConnection(
                        uuid=self._uuid,
                        name=self._name,
                        websocket=self._websocket,
                    )
                    async with self._peers_lock:
                        self._peers[peers] = connection
                    self._tasks[peers] = spawn_guarded_background_task(
                        self._handle_peer_messages,
                        message.source_uuid,
                        connection,
                    )
                await self._peers[peers].handle_server_message(message)
            elif isinstance(message, messages.ServerResponse):
                # The peer manager should never send something to the
                # signaling server that warrants a ServerResponse
                logger.exception(
                    f'{self._log_prefix}: got unexpected ServerResponse '
                    f'from signaling server: {message}',
                )
            else:
                logger.error(
                    f'{self._log_prefix}: received unknown message type '
                    f'{type(message).__name__} from signaling server',
                )

    async def close(self) -> None:
        """Close the connection manager."""
        if self._server_task is not None:
            self._server_task.cancel()
        for task in self._tasks.values():
            task.cancel()
        async with self._peers_lock:
            for connection in self._peers.values():
                await connection.close()
        if self._websocket_or_none is not None:
            await self._websocket_or_none.close()
        logger.info(f'{self._log_prefix}: peer manager closed')

    async def recv(self) -> messages.PeerMessage:
        """Receive next message from a peer.

        Returns:
            message received from peer.
        """
        return await self._message_queue.get()

    async def send(
        self,
        peer_uuid: UUID,
        message: messages.PeerMessage,
        timeout: float = 30,
    ) -> None:
        """Send message to peer.

        Args:
            peer_uuid (str): UUID of peer to send message to.
            message (PeerMessage): message to send to peer.
            timeout (float): timeout to wait on peer connection to be ready.

        Raises:
            PeerConnectionTimeout:
                if the peer connection is not established within the timeout.
        """
        connection = await self.get_connection(peer_uuid)
        message_str = messages.encode(message)
        await connection.send(message_str, timeout)

    async def get_connection(self, peer_uuid: UUID) -> PeerConnection:
        """Get connection to the peer.

        Args:
            peer_uuid (str, UUID): uuid of peer to make connection with.

        Returns:
            :any:`PeerConnection <proxystore.p2p.connection.PeerConnection>`
        """
        peers = frozenset({self._uuid, peer_uuid})

        async with self._peers_lock:
            if peers in self._peers:
                return self._peers[peers]

            connection = PeerConnection(
                self._uuid,
                self._name,
                self._websocket,
            )
            self._peers[peers] = connection

        logger.info(
            f'{self._log_prefix}: opening peer connection with '
            f'{peer_uuid}',
        )
        await connection.send_offer(peer_uuid)

        self._tasks[peers] = spawn_guarded_background_task(
            self._handle_peer_messages,
            peer_uuid,
            connection,
        )
        return connection

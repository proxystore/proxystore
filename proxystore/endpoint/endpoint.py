"""ProxyStore Endpoint."""
from __future__ import annotations

import asyncio
import enum
import logging
import uuid
from types import TracebackType
from typing import Any
from typing import Generator

from websockets import WebSocketServerProtocol

import proxystore.endpoint.messages as messages
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.server import connect
from proxystore.p2p.task import spawn_guarded_background_task

logger = logging.getLogger(__name__)


class EndpointMode(enum.Enum):
    """Endpoint mode."""

    PEERING = 1
    """Endpoint will establish peer connections with other endpoints."""
    SOLO = 2
    """Endpoint is operating in isolation and will ignore peer requests."""


class Endpoint:
    """ProxyStore Endpoint.

    Endpoints act as distributed blob stores. Endpoints support peer-to-peer
    communication for retrieving data not located on the local endpoint.

    Endpoints have two modes: :py:attr:`SOLO <.EndpointMode.SOLO>` and
    :py:attr:`PEERING <.EndpointMode.PEERING>` indicating if the endpoint is
    initialized with a signaling server address to establish peer to peer
    connections with other endpoints.

    Warning:
        Requests made to remote endpoints will only invoke the request on
        the remote and return the result. I.e., invoking GET on a remote
        will return the value but will not store it on the local endpoint.
    """

    def __init__(
        self,
        uuid: str | None = None,
        signaling_server: str | None = None,
        peer_timeout: int = 30,
    ) -> None:
        """Init Endpoint.

        Args:
            uuid (str, optional): uuid of the endpoint. If not provided,
                a UUID will be request from the signaling server.
            signaling_server (str, optional): address of signaling
                server used for peer-to-peer connections between endpoints. If
                None, endpoint will not be able to communicate with other
                endpoints (default: None).
            peer_timeout (int): timeout for establishing p2p connection with
                another endpoint (default: 30).
        """
        # TODO(gpauloski): endpoint needs to have a UUID immediately
        # TODO(gpauloski): need to consider semantics of operations
        #   - can all ops be triggered on remote?
        #   - or just get? do we move data permanently on get? etc...
        self._uuid = uuid
        self._signaling_server = signaling_server
        self._peer_timeout = peer_timeout

        self._mode = (
            EndpointMode.SOLO
            if signaling_server is None
            else EndpointMode.PEERING
        )

        self._peer_manager: PeerManager | None = None
        self._signaling_server_socket: WebSocketServerProtocol | None = None

        self._data: dict[str, bytes] = {}
        self._pending_requests: dict[
            str,
            asyncio.Future[messages.Response],
        ] = {}

        self._peer_handler_task: asyncio.Task[None] | None = None

        # TODO(gpauloski): logging

    @property
    def uuid(self) -> str | None:
        """Get UUID of this endpoint."""
        return self._uuid

    async def __aenter__(self) -> Endpoint:
        """Enter async context manager."""
        await self.async_init()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Exit async context manager."""
        await self.close()

    def __await__(self) -> Generator[Any, None, Endpoint]:
        """Initialize endpoint awaitables."""
        return self.__aenter__().__await__()

    async def async_init(self) -> None:
        """Initialize endpoint awaitables.

        Note:
            Typically, the endpoint should be used as a context manager
            or initialized with await.

            >>> endpoint = await Endpoint(...)
            >>> async with Endpoint(...) as endpoint:
            >>>     ...
        """
        if self._signaling_server is not None:
            self._uuid, self._signaling_server_socket = await connect(
                self._signaling_server,
            )
            self._peer_manager = PeerManager(
                self._uuid,
                self._signaling_server_socket,
            )
            self._peer_handler_task = spawn_guarded_background_task(
                self._handle_peer_requests,
            )

    async def _handle_peer_requests(self) -> None:
        """Coroutine to listen for request from peer endpoints."""
        assert self._peer_manager is not None

        while True:
            source_endpoint, message = await self._peer_manager.recv()

            if not isinstance(message, messages.Request):
                logger.error(
                    f'received unsupported message type {type(message)} from '
                    f'peer endpoint {source_endpoint}',
                )
                continue

            if message._id is None:
                logger.error('got request from peer endpoint with no ID')
                continue

            if isinstance(message, messages.Response):
                if message._id not in self._pending_requests:
                    logger.error(
                        f'received {type(message)} with id {message._id} '
                        'that does not match a pending request.',
                    )
                else:
                    self._pending_requests[message._id].set_result(message)
                    del self._pending_requests[message._id]
                continue

            response: messages.Response
            if isinstance(message, messages.EvictRequest):
                await self.evict(message.key)
                response = messages.EvictResponse(
                    key=message.key,
                    success=True,
                    _id=message._id,
                )
            elif isinstance(message, messages.ExistsRequest):
                exists = await self.exists(message.key)
                response = messages.ExistsResponse(
                    key=message.key,
                    exists=exists,
                    _id=message._id,
                )
            elif isinstance(message, messages.GetRequest):
                data = await self.get(message.key)
                response = messages.GetResponse(
                    key=message.key,
                    data=data,
                    _id=message._id,
                )
            elif isinstance(message, messages.SetRequest):
                assert message.data is not None
                await self.set(message.key, message.data)
                response = messages.SetResponse(
                    key=message.key,
                    success=True,
                    _id=message._id,
                )
            else:
                raise AssertionError(
                    f'unsupported request type {type(message)}',
                )

            await self._peer_manager.send(source_endpoint, response)

    async def _request_from_peer(
        self,
        endpoint: str,
        request: messages.Request,
    ) -> asyncio.Future[messages.Response]:
        """Send request to peer endpoint."""
        # TODO(gpauloski):
        #   - should some ops be sent to all endpoints that may have
        #     a copy of the data (mostly for evict)?
        assert self._peer_manager is not None
        request._id = str(uuid.uuid4())
        self._pending_requests[request._id] = asyncio.Future()
        await self._peer_manager.send(endpoint, request)
        return self._pending_requests[request._id]

    def _is_peer_request(self, endpoint: str | None) -> bool:
        """Check if this request should be forwarded to peer endpoint."""
        if self._mode == EndpointMode.SOLO:
            return False
        elif endpoint is None or endpoint == self._uuid:
            return False
        elif self._peer_manager is None:
            raise PeeringNotAvailableError(
                'P2P connection manager has not been enabled yet. Try '
                'initializing the endpoint with endpoint = await '
                'Endpoint(...) or calling endpoint.async_init().',
            )
        else:
            return True

    async def evict(self, key: str, endpoint: str | None = None) -> None:
        """Evict key from endpoint.

        Args:
            key (str): key to evict.
            endpoint (optional, str): endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.
        """
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = messages.EvictRequest(key=key)
            request_future = await self._request_from_peer(endpoint, request)
            await request_future
            # TODO(gpauloski): check future for failure?
        else:
            if key in self._data:
                del self._data[key]

    async def exists(self, key: str, endpoint: str | None = None) -> bool:
        """Check if key exists on endpoint.

        Args:
            key (str): key to check.
            endpoint (optional, str): endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.

        Returns:
            True if key exists.
        """
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = messages.ExistsRequest(key=key)
            request_future = await self._request_from_peer(endpoint, request)
            response = await request_future
            assert isinstance(response, messages.ExistsResponse)
            assert isinstance(response.exists, bool)
            return response.exists
        else:
            return key in self._data

    async def get(self, key: str, endpoint: str | None = None) -> bytes | None:
        """Get value associated with key on endpoint.

        Args:
            key (str): key to get value for.
            endpoint (optional, str): endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.

        Returns:
            value (bytes) associated with key.
        """
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = messages.GetRequest(key=key)
            request_future = await self._request_from_peer(endpoint, request)
            response = await request_future
            assert isinstance(response, messages.GetResponse)
            return response.data
        else:
            if key in self._data:
                return self._data[key]
            else:
                return None

    async def set(
        self,
        key: str,
        data: bytes,
        endpoint: str | None = None,
    ) -> None:
        """Set key with data on endpoint.

        Args:
            key (str): key to associate with value.
            data (bytes): value to associate with key.
            endpoint (optional, str): endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.
        """
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = messages.SetRequest(key=key, data=data)
            request_future = await self._request_from_peer(endpoint, request)
            await request_future
        else:
            self._data[key] = data

    async def close(self) -> None:
        """Close the endpoint and any open connections safely."""
        if self._peer_handler_task is not None:
            self._peer_handler_task.cancel()
        if self._peer_manager is not None:
            await self._peer_manager.close()
        if self._signaling_server_socket is not None:
            await self._signaling_server_socket.close()

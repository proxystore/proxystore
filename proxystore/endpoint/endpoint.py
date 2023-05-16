"""Endpoint implementation."""
from __future__ import annotations

import asyncio
import enum
import logging
from types import TracebackType
from typing import Any
from typing import Generator
from uuid import UUID
from uuid import uuid4

from proxystore.endpoint.constants import MAX_OBJECT_SIZE_DEFAULT
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.messages import EndpointRequest
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import Storage
from proxystore.p2p.connection import log_name
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.task import spawn_guarded_background_task
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize
from proxystore.utils import bytes_to_readable

logger = logging.getLogger(__name__)


class EndpointMode(enum.Enum):
    """Endpoint mode."""

    PEERING = 1
    """Endpoint will establish peer connections with other endpoints."""
    SOLO = 2
    """Endpoint is operating in isolation and will ignore peer requests."""


class Endpoint:
    """ProxyStore Endpoint.

    An endpoint is an object store with `get`/`set` functionality.

    By default, an endpoint operates in
    [`EndpointMode.SOLO`][proxystore.endpoint.endpoint.EndpointMode.SOLO]
    mode where the endpoint acts just as an isolated object store. Endpoints
    can also be configured in
    [`EndpointMode.PEERING`][proxystore.endpoint.endpoint.EndpointMode.PEERING]
    mode by initializing the endpoint with a relay server address.
    The relay server is used to establish peer-to-peer connections with
    other endpoints after which endpoints can forward operations between each
    other. Peering is available even when endpoints are being separate
    NATs. See the [proxystore.p2p][] module to learn more about peering.

    Warning:
        Requests made to remote endpoints will only invoke the request on
        the remote and return the result. I.e., invoking GET on a remote
        will return the value but will not store it on the local endpoint.

    Example:
        Solo Mode Usage

        ```python
        async with Endpoint('ep1', uuid.uuid4()) as endpoint:
            serialized_data = b'data string'
            endpoint.set('key', serialized_data)
            assert endpoint.get('key') == serialized_data
            endpoint.evict('key')
            assert not endpoint.exists('key')
        ```

    Example:
        Peering Mode Usage

        ```python
        ep1 = await Endpoint('ep1', uuid.uuid4(), relay_server)
        ep2 = await Endpoint('ep1', uuid.uuid4(), relay_server)

        serialized_data = b'data string'
        ep1.set('key', serialized_data)
        assert ep2.get('key', endpoint=ep1.uuid) == serialized_data
        assert ep1.exists('key')
        assert not ep1.exists('key', endpoint=ep2.uuid)

        ep1.close()
        ep2.close()
        ```

    Note:
        Endpoints can be configured and started via the
        `proxystore-endpoint` command-line interface.


    Note:
        If the endpoint is being used in peering mode, the endpoint should be
        used as a context manager or initialized with await. This will ensure
        [`Endpoint.async_init()`][proxystore.endpoint.endpoint.Endpoint.async_init]
        is executed which connects to the relay server and established a
        listener for incoming messages.

        ```python
        endpoint = await Endpoint(...)
        endpoint.close()
        ```

        ```python
        async with Endpoint(...) as endpoint:
            ...
        ```

    Args:
        name: Readable name of endpoint.
        uuid: UUID of the endpoint.
        relay_server: Address of relay server used for peer-to-peer
            connections between endpoints. If None, endpoint will not be able
            to communicate with other endpoints.
        max_object_size: Optional max size in bytes for any single
            object stored by the endpoint. If exceeded, an error is raised.
        peer_channels: Number of datachannels per peer connection
            to another endpoint to communicate over.
        peer_timeout: Timeout for establishing p2p connection with
            another endpoint.
        storage: Storage interface to use. If `None`,
            [`DictStorage`][proxystore.endpoint.storage.DictStorage] is used.
        verify_certificate: Verify the relay server's SSL
            certificate. This should almost never be disabled except for
            testing with self-signed certificates.
    """

    def __init__(
        self,
        name: str,
        uuid: UUID,
        relay_server: str | None = None,
        *,
        max_object_size: int | None = MAX_OBJECT_SIZE_DEFAULT,
        peer_channels: int = 1,
        peer_timeout: int = 30,
        storage: Storage | None = None,
        verify_certificate: bool = True,
    ) -> None:
        self._name = name
        self._uuid = uuid
        self._relay_server = relay_server
        self._max_object_size = max_object_size
        self._peer_channels = peer_channels
        self._peer_timeout = peer_timeout
        self._verify_certificate = verify_certificate

        self._storage = DictStorage() if storage is None else storage

        self._mode = (
            EndpointMode.SOLO if relay_server is None else EndpointMode.PEERING
        )
        self._peer_manager: PeerManager | None = None
        self._pending_requests: dict[
            str,
            asyncio.Future[EndpointRequest],
        ] = {}
        self._async_init_done = False
        self._peer_handler_task: asyncio.Task[None] | None = None

        logger.info(
            f'{self._log_prefix}: initialized endpoint operating '
            f'in {self._mode.name} mode',
        )

    @property
    def _log_prefix(self) -> str:
        return f'{type(self).__name__}[{log_name(self.uuid, self.name)}]'

    @property
    def uuid(self) -> UUID:
        """UUID of this endpoint."""
        return self._uuid

    @property
    def name(self) -> str:
        """Name of this endpoint."""
        return self._name

    async def __aenter__(self) -> Endpoint:
        await self.async_init()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def __await__(self) -> Generator[Any, None, Endpoint]:
        return self.__aenter__().__await__()

    async def async_init(self) -> None:
        """Initialize connections and tasks necessary for peering."""
        if self._relay_server is not None and not self._async_init_done:
            self._peer_manager = await PeerManager(
                uuid=self.uuid,
                relay_server=self._relay_server,
                name=self.name,
                timeout=self._peer_timeout,
                peer_channels=self._peer_channels,
                verify_certificate=self._verify_certificate,
            )
            self._peer_handler_task = spawn_guarded_background_task(
                self._handle_peer_requests,
            )
            logger.info(f'{self._log_prefix}: initialized peer manager')
            self._async_init_done = True

    async def _handle_peer_requests(self) -> None:
        """Coroutine to listen for request from peer endpoints."""
        assert self._peer_manager is not None
        logger.info(f'{self._log_prefix}: listening for peer requests')

        while True:
            source_endpoint, message_ = await self._peer_manager.recv()
            assert isinstance(message_, bytes)
            try:
                message: EndpointRequest = deserialize(message_)
            except SerializationError as e:
                logger.error(
                    f'{self._log_prefix}: unable to decode message from peer '
                    f'endpoint {source_endpoint}: {e}',
                )
                continue

            if message.kind == 'response':
                if message.uuid not in self._pending_requests:
                    logger.error(
                        f'{self._log_prefix}: received '
                        f'{type(message).__name__} with ID {message.uuid} '
                        'that does not match a pending request',
                    )
                else:
                    fut = self._pending_requests.pop(message.uuid)
                    if message.error is None:
                        fut.set_result(message)
                    else:
                        fut.set_exception(message.error)
                continue

            logger.debug(
                f'{self._log_prefix}: received {type(message).__name__}'
                f'(id={message.uuid}, key={message.key}) from '
                f'{source_endpoint}',
            )

            try:
                if message.op == 'evict':
                    await self.evict(message.key)
                elif message.op == 'exists':
                    message.exists = await self.exists(message.key)
                elif message.op == 'get':
                    message.data = await self.get(message.key)
                elif message.op == 'set':
                    assert message.data is not None
                    await self.set(message.key, message.data)
                    message.data = None
                else:
                    raise AssertionError(
                        f'unsupported request type {type(message).__name__}',
                    )
            except Exception as e:
                message.error = e

            message.kind = 'response'
            logger.debug(
                f'{self._log_prefix}: sending {message.op} response with '
                f'id={message.uuid} and key={message.key} to '
                f'{source_endpoint}',
            )
            await self._peer_manager.send(source_endpoint, serialize(message))

    async def _request_from_peer(
        self,
        endpoint: UUID,
        request: EndpointRequest,
    ) -> asyncio.Future[EndpointRequest]:
        """Send request to peer endpoint.

        Any exceptions will be set on the returned future.
        """
        # TODO(gpauloski):
        #   - should some ops be sent to all endpoints that may have
        #     a copy of the data (mostly for evict)?
        assert self._peer_manager is not None
        self._pending_requests[
            request.uuid
        ] = asyncio.get_running_loop().create_future()
        logger.debug(
            f'{self._log_prefix}: sending {request.op} request with '
            f'id={request.uuid} and key={request.key}) to {endpoint}',
        )
        try:
            await self._peer_manager.send(endpoint, serialize(request))
        except Exception as e:
            self._pending_requests[request.uuid].set_exception(
                PeerRequestError(
                    f'Request to peer {endpoint} failed: {e!s}',
                ),
            )
        return self._pending_requests[request.uuid]

    def _is_peer_request(self, endpoint: UUID | None) -> bool:
        """Check if this request should be forwarded to peer endpoint."""
        if self._mode == EndpointMode.SOLO:
            return False
        elif endpoint is None or endpoint == self.uuid:
            return False
        elif self._peer_manager is None:
            raise PeeringNotAvailableError(
                'P2P connection manager has not been enabled yet. Try '
                'initializing the endpoint with endpoint = await '
                'Endpoint(...) or calling endpoint.async_init().',
            )
        else:
            return True

    async def evict(self, key: str, endpoint: UUID | None = None) -> None:
        """Evict key from endpoint.

        Args:
            key: Key to evict.
            endpoint: Endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.

        Raises:
            PeerRequestError: If request to a peer endpoint fails.
        """
        logger.debug(
            f'{self._log_prefix}: EVICT key={key} on endpoint={endpoint}',
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = EndpointRequest(
                kind='request',
                op='evict',
                uuid=str(uuid4()),
                key=key,
            )
            request_future = await self._request_from_peer(endpoint, request)
            await request_future
        else:
            await self._storage.evict(key)

    async def exists(self, key: str, endpoint: UUID | None = None) -> bool:
        """Check if key exists on endpoint.

        Args:
            key: Key to check.
            endpoint: Endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.

        Returns:
            If the key exists.

        Raises:
            PeerRequestError: If request to a peer endpoint fails.
        """
        logger.debug(
            f'{self._log_prefix}: EXISTS key={key} on endpoint={endpoint}',
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = EndpointRequest(
                kind='request',
                op='exists',
                uuid=str(uuid4()),
                key=key,
            )
            request_future = await self._request_from_peer(endpoint, request)
            response = await request_future
            assert isinstance(response.exists, bool)
            return response.exists
        else:
            return await self._storage.exists(key)

    async def get(
        self,
        key: str,
        endpoint: UUID | None = None,
    ) -> bytes | None:
        """Get value associated with key on endpoint.

        Args:
            key: Key to get value for.
            endpoint: Endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.

        Returns:
            Value associated with key.

        Raises:
            PeerRequestError: If request to a peer endpoint fails.
        """
        logger.debug(
            f'{self._log_prefix}: GET key={key} on endpoint={endpoint}',
        )
        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = EndpointRequest(
                kind='request',
                op='get',
                uuid=str(uuid4()),
                key=key,
            )
            request_future = await self._request_from_peer(endpoint, request)
            response = await request_future
            return response.data
        else:
            return await self._storage.get(key, None)

    async def set(
        self,
        key: str,
        data: bytes,
        endpoint: UUID | None = None,
    ) -> None:
        """Set key with data on endpoint.

        Args:
            key: Key to associate with value.
            data: Value to associate with key.
            endpoint: Endpoint to perform operation on. If
                unspecified or if the endpoint is on solo mode, the operation
                will be performed on the local endpoint.

        Raises:
            ObjectSizeExceededError: If the max object size is configured and
                the data exceeds that size.
            PeerRequestError: If request to a peer endpoint fails.
        """
        logger.debug(
            f'{self._log_prefix}: SET key={key} on endpoint={endpoint}',
        )

        if self._is_peer_request(endpoint):
            assert endpoint is not None
            request = EndpointRequest(
                kind='request',
                op='set',
                uuid=str(uuid4()),
                key=key,
                data=data,
            )
            request_future = await self._request_from_peer(endpoint, request)
            await request_future
        elif (
            self._max_object_size is not None
            and len(data) > self._max_object_size
        ):
            raise ObjectSizeExceededError(
                f'Bytes value has size {bytes_to_readable(len(data))} which '
                f'exceeds the {bytes_to_readable(self._max_object_size)} '
                'object limit.',
            )
        else:
            await self._storage.set(key, data)

    async def close(self) -> None:
        """Close the endpoint and any open connections safely."""
        if self._peer_handler_task is not None:
            self._peer_handler_task.cancel()
            try:
                await self._peer_handler_task
            except asyncio.CancelledError:
                pass
        if self._peer_manager is not None:
            await self._peer_manager.close()
        await self._storage.close()
        logger.info(f'{self._log_prefix}: endpoint closed')

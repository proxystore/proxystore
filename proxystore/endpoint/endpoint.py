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

from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.messages import EndpointRequest
from proxystore.endpoint.storage import DictStorage
from proxystore.endpoint.storage import Storage
from proxystore.p2p.connection import log_name
from proxystore.p2p.manager import PeerManager
from proxystore.serialize import deserialize
from proxystore.serialize import SerializationError
from proxystore.serialize import serialize
from proxystore.utils.tasks import spawn_guarded_background_task

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
    mode by initializing the endpoint with a
    [`PeerManager`][proxystore.p2p.manager.PeerManager].
    The [`PeerManager`][proxystore.p2p.manager.PeerManager] is connected to a
    relay server which is used to establish peer-to-peer connections with
    other endpoints connected to the same relay server. After peer connections
    are established, endpoints can forward operations between each
    other. Peering is available even when endpoints are behind separate
    NATs. See the [`proxystore.p2p`][proxystore.p2p] module to learn more
    about peering.

    Warning:
        Requests made to remote endpoints will only invoke the request on
        the remote and return the result. I.e., invoking GET on a remote
        will return the value but will not store it on the local endpoint.

    Example:
        Solo Mode Usage

        ```python
        async with Endpoint('ep1', uuid.uuid4()) as endpoint:
            serialized_data = b'data string'
            await endpoint.set('key', serialized_data)
            assert await endpoint.get('key') == serialized_data
            await endpoint.evict('key')
            assert not await endpoint.exists('key')
        ```

    Example:
        Peering Mode Usage

        ```python
        pm1 = await PeerManager(RelayClient(...))
        pm2 = await PeerManager(RelayClient(...))
        ep1 = await Endpoint(peer_manager=pm1)
        ep2 = await Endpoint(peer_manager=pm2)

        serialized_data = b'data string'
        await ep1.set('key', serialized_data)
        assert await ep2.get('key', endpoint=ep1.uuid) == serialized_data
        assert await ep1.exists('key')
        assert not await ep1.exists('key', endpoint=ep2.uuid)

        await ep1.close()
        await ep2.close()
        ```

    Note:
        Endpoints can be configured and started via the
        [`proxystore-endpoint`](../cli.md#proxystore-endpoint) command-line
        interface.


    Note:
        If the endpoint is being used in peering mode, the endpoint should be
        used as a context manager or initialized with await. This will ensure
        [`Endpoint.async_init()`][proxystore.endpoint.endpoint.Endpoint.async_init]
        is called which initializes the background task that listens for
        incoming peer messages.

        ```python
        endpoint = await Endpoint(...)
        await endpoint.close()
        ```

        ```python
        async with Endpoint(...) as endpoint:
            ...
        ```

    Args:
        name: Readable name of the endpoint. Only used if `peer_manager` is not
            provided. Otherwise the name will be set to
            [`PeerManager.name`][proxystore.p2p.manager.PeerManager.name].
        uuid: UUID of the endpoint. Only used if `peer_manager` is not
            provided. Otherwise the UUID will be set to
            [`PeerManager.uuid`][proxystore.p2p.manager.PeerManager.uuid].
        peer_manager: Optional peer manager that is connected to a relay server
            which will be used for establishing peer connections to other
            endpoints connected to the same relay server.
        storage: Storage interface to use. If `None`,
            [`DictStorage`][proxystore.endpoint.storage.DictStorage] is used.

    Raises:
        ValueError: if neither `name`/`uuid` or `peer_manager` are set.
    """

    def __init__(
        self,
        name: str | None = None,
        uuid: UUID | None = None,
        *,
        peer_manager: PeerManager | None = None,
        storage: Storage | None = None,
    ) -> None:
        if peer_manager is None and (name is None or uuid is None):
            raise ValueError(
                'The name and uuid parameters must be provided if '
                'a PeerManager is not provided.',
            )

        self._default_name = name
        self._default_uuid = uuid
        self._peer_manager = peer_manager
        self._storage = DictStorage() if storage is None else storage

        self._mode = (
            EndpointMode.SOLO if peer_manager is None else EndpointMode.PEERING
        )
        self._pending_requests: dict[
            str,
            asyncio.Future[EndpointRequest],
        ] = {}
        self._peer_handler_task: asyncio.Task[None] | None = None

        if self._mode is EndpointMode.SOLO:
            # Initialization is not complete for endpoints in peering mode
            # until async_init() is called.
            logger.info(
                f'{self._log_prefix}: initialized endpoint operating '
                f'in {self._mode.name} mode',
            )

    @property
    def _log_prefix(self) -> str:
        return f'{type(self).__name__}[{log_name(self.uuid, self.name)}]'

    @property
    def name(self) -> str:
        """Name of this endpoint."""
        if self._mode is EndpointMode.SOLO:
            assert self._default_name is not None
            return self._default_name
        elif self._mode is EndpointMode.PEERING:
            assert self.peer_manager is not None
            return self.peer_manager.name
        else:
            raise AssertionError('Unreachable.')

    @property
    def uuid(self) -> UUID:
        """UUID of this endpoint."""
        if self._mode is EndpointMode.SOLO:
            assert self._default_uuid is not None
            return self._default_uuid
        elif self._mode is EndpointMode.PEERING:
            assert self.peer_manager is not None
            return self.peer_manager.uuid
        else:
            raise AssertionError('Unreachable.')

    @property
    def peer_manager(self) -> PeerManager | None:
        """Peer manager.

        Raises:
            PeeringNotAvailableError: if the endpoint was initialized with
                a [`PeerManager`][proxystore.p2p.manager.PeerManager] but
                [`Endpoint.async_init()`][proxystore.endpoint.endpoint.Endpoint.async_init]
                has not been called. This is likely because the endpoint was
                not initialized with the `await` keyword.
        """
        if self._peer_manager is not None and self._peer_handler_task is None:
            raise PeeringNotAvailableError(
                'The peer message handler has not been created yet. '
                'This is likely because async_init() has not been called. '
                'Is the endpoint being initialized with await?',
            )
        return self._peer_manager

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
        """Initialize connections and tasks necessary for peering.

        Note:
            This will also call
            [`PeerManager.async_init()`][proxystore.p2p.manager.PeerManager.async_init]
            if one is provided so that asynchronous resources for both the
            [`PeerManager`][proxystore.p2p.manager.PeerManager]
            and endpoint can be initialized later after creation.
        """
        if self._peer_manager is not None and self._peer_handler_task is None:
            await self._peer_manager.async_init()
            self._peer_handler_task = spawn_guarded_background_task(
                self._handle_peer_requests,
            )
            self._peer_handler_task.set_name(
                f'endpoint-{self.uuid}-handle-peer-requests',
            )
            logger.info(
                f'{self._log_prefix}: initialized endpoint operating '
                f'in {self._mode.name} mode',
            )

    async def _handle_peer_requests(self) -> None:  # noqa: C901
        """Coroutine to listen for request from peer endpoints."""
        assert self.peer_manager is not None
        logger.info(f'{self._log_prefix}: listening for peer requests')

        while True:
            source_endpoint, message_ = await self.peer_manager.recv()
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
            await self.peer_manager.send(source_endpoint, serialize(message))

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
        assert self.peer_manager is not None
        self._pending_requests[
            request.uuid
        ] = asyncio.get_running_loop().create_future()
        logger.debug(
            f'{self._log_prefix}: sending {request.op} request with '
            f'id={request.uuid} and key={request.key}) to {endpoint}',
        )
        try:
            await self.peer_manager.send(endpoint, serialize(request))
        except Exception as e:
            self._pending_requests[request.uuid].set_exception(
                PeerRequestError(
                    f'Request to peer {endpoint} failed: {e!s}',
                ),
            )
        return self._pending_requests[request.uuid]

    def _is_peer_request(self, endpoint: UUID | None) -> bool:
        """Check if this request should be forwarded to peer endpoint."""
        return not (
            self._mode == EndpointMode.SOLO
            or endpoint is None
            or endpoint == self.uuid
        )

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

"""Client interface to a relay server."""
from __future__ import annotations

import asyncio
import logging
import ssl
import sys
import uuid
from types import TracebackType

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

try:
    import websockets
    import websockets.exceptions
    from websockets.client import WebSocketClientProtocol
except ImportError as e:  # pragma: no cover
    import warnings

    warnings.warn(
        f'{e}. To enable p2p endpoint connections, install proxystore with '
        '"pip install proxystore[endpoints]".',
        stacklevel=2,
    )

from proxystore.p2p.relay.exceptions import RelayNotConnectedError
from proxystore.p2p.relay.exceptions import RelayRegistrationError
from proxystore.p2p.relay.messages import decode_relay_message
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import RelayMessage
from proxystore.p2p.relay.messages import RelayMessageDecodeError
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse
from proxystore.utils.environment import hostname
from proxystore.utils.tasks import spawn_guarded_background_task

logger = logging.getLogger(__name__)


class RelayClient:
    """Client interface to a relay server.

    This interface abstracts the low-level WebSocket connection to a
    relay server to provide automatic reconnection.

    Tip:
        This class can be used as an async context manager!
        ```python
        from proxystore.p2p.relay.client import RelayClient

        async with RelayClient(...) as client:
            await client.send(...)
            message = await client.recv(...)
        ```

    Note:
        WebSocket connections are not opened until a message is sent,
        a message is received, or
        [`connect()`][proxystore.p2p.relay.client.RelayClient.connect]
        is called. Initializing the client with `await` will call
        [`connect()`][proxystore.p2p.relay.client.RelayClient.connect].
        ```python
        client = await RelayClient(...)
        ```

    Args:
        address: Address of the relay server. Should start with `ws://` or
            `wss://`.
        client_name: Optional name of the client to use when registering with
            the relay server. If `None`, the hostname will be used.
        client_uuid: Optional UUID of the client to use when registering with
            the relay server. If `None`, one will be generated.
        extra_headers: Arbitrary HTTP headers to add to the handshake request.
            If connecting to a relay server with authentication, such as
            one using the
            [`GlobusAuthenticator`][proxystore.p2p.relay.authenticate.GlobusAuthenticator],
            the headers should include the `Authorization` header containing
            the bearer token.
        reconnect_task: Spawn a background task which will automatically
            reconnect to the relay server when the websocket client closes.
            Otherwise, reconnections will only be attempted when sending or
            receiving a message.
        ssl_context: Custom SSL context to pass to
            [`websockets.connect()`][websockets.client.connect]. A TLS context
            is created with
            [`ssl.create_default_context()`][ssl.create_default_context]
            when connecting to a `wss://` URI and `ssl_context` is not
            provided.
        timeout: Time to wait in seconds on relay server connection.
        verify_certificate: Verify the relay server's SSL certificate. Only
            used if `ssl_context` is `None` and connecting to a `wss://` URI.

    Raises:
        RelayRegistrationError: If the connection to the relay server
            is closed, does not reply to the registration request within the
            timeout, or replies with an error.
        ValueError: If address does not start with `ws://` or `wss://`.
    """

    def __init__(
        self,
        address: str,
        *,
        client_name: str | None = None,
        client_uuid: uuid.UUID | None = None,
        extra_headers: dict[str, str] | None = None,
        reconnect_task: bool = True,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10,
        verify_certificate: bool = True,
    ) -> None:
        if not (address.startswith('ws://') or address.startswith('wss://')):
            raise ValueError(
                'Relay server address must start with ws:// or wss://.'
                f'Got {address}.',
            )

        self._address = address
        self._name = hostname() if client_name is None else client_name
        self._uuid = uuid.uuid4() if client_uuid is None else client_uuid
        self._timeout = timeout

        if self._address.startswith('wss://') and ssl_context is None:
            ssl_context = ssl.create_default_context()
            if not verify_certificate:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

        self._extra_headers = extra_headers
        self._ssl_context = ssl_context
        self._create_reconnect_task = reconnect_task

        self._initial_backoff_seconds = 1.0

        self._connect_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task[None] | None = None
        self._websocket: WebSocketClientProtocol | None = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def _register(self, timeout: float) -> WebSocketClientProtocol:
        """Open a websocket connection and register with the relay server.

        Args:
            timeout: Timeout to wait on opening the initial connection and
                waiting for a server response.

        Returns:
            Open websocket connection with the relay server.

        Raises:
            ConnectionRefusedError: If the server could not be connected to.
            asyncio.TimeoutError: If the server did not reply within the
                timeout.
            websockets.exceptions.ConnectionClosed: If the websocket connection
                was closed while registering.
            RelayRegistrationError: If the registration process failed.
        """
        websocket = await websockets.client.connect(
            self._address,
            open_timeout=timeout,
            ssl=self._ssl_context,
            extra_headers=self._extra_headers,
        )

        registration_message = RelayRegistrationRequest(self.name, self.uuid)
        await websocket.send(encode_relay_message(registration_message))

        try:
            message_str = await asyncio.wait_for(
                websocket.recv(),
                timeout,
            )
            if isinstance(message_str, str):
                message = decode_relay_message(message_str)
            else:
                raise AssertionError('Received non-string type on websocket.')
        except RelayMessageDecodeError as e:
            raise RelayRegistrationError(
                'Unable to decode response message from relay server.',
            ) from e

        if isinstance(message, RelayResponse):
            if message.success:
                logger.info(
                    'Established client connection to relay server at '
                    f'{self._address} with client uuid={self.uuid} '
                    f'and name={self.name}',
                )
                return websocket
            else:
                raise RelayRegistrationError(
                    'Failed to register as peer with the relay server. '
                    f'Got exception: {message.message}',
                )
        else:
            raise RelayRegistrationError(
                'Relay server replied with unknown message type: '
                f'{type(message).__name__}.',
            )

    async def _reconnect_on_close(self) -> None:
        """Wait for websocket to close and immediately reconnect.

        This is intended to be run as an asyncio tasks and should only
        be started after the websocket connection has been created.
        """
        assert self._websocket is not None
        while True:
            await self._websocket.wait_closed()
            assert self._websocket.closed
            await self.connect()

    @property
    def name(self) -> str:
        """Name of client as registered with relay server."""
        return self._name

    @property
    def uuid(self) -> uuid.UUID:
        """UUID of client as registered with relay server."""
        return self._uuid

    @property
    def websocket(self) -> WebSocketClientProtocol:
        """Websocket connection to the relay server.

        Raises:
            RelayNotConnectedError: if the websocket connection to the relay
                server is not open. This usually indicates that
                [`connect()`][proxystore.p2p.relay.client.RelayClient.connect]
                needs to be called.
        """
        if self._websocket is not None and self._websocket.open:
            return self._websocket
        else:
            raise RelayNotConnectedError(
                'Websocket connection to the relay server is not open. '
                'Try calling connect() first.',
            )

    async def connect(self, retry: bool = True) -> None:
        """Connect to the relay server.

        Note:
            Typically this does not need to be called because the
            send and receive methods will automatically call this.

        Note:
            This method is a no-op if a connection is already established.
            Otherwise, a new connection will be attempted with
            exponential backoff when `retry` is True for connection failures.

        Args:
            retry: Retry the connection with exponential backoff starting at
                one second and increasing to a max of 60 seconds.
        """
        async with self._connect_lock:
            if self._websocket is not None and self._websocket.open:
                return

            backoff_seconds = self._initial_backoff_seconds
            while True:
                try:
                    self._websocket = await self._register(
                        timeout=self._timeout,
                    )
                    if (
                        self._reconnect_task is None
                        and self._create_reconnect_task
                    ):
                        self._reconnect_task = spawn_guarded_background_task(
                            self._reconnect_on_close,
                        )
                        self._reconnect_task.set_name('relay-client-reconnect')
                except (
                    # Exceptions that we should wait and retry again for
                    ConnectionRefusedError,
                    asyncio.TimeoutError,
                    websockets.exceptions.ConnectionClosed,
                ) as e:
                    if not retry:
                        raise

                    logger.warning(
                        f'Registration with relay server at {self._address} '
                        f'failed because of {e}. Retrying connection in '
                        f'{backoff_seconds} seconds',
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds = min(backoff_seconds * 2, 60)
                else:
                    # Coverage doesn't detect the singular break but it does
                    # get executed to break from the loop
                    break  # pragma: no cover

    async def close(self) -> None:
        """Close the connection to the relay server."""
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        if self._websocket is not None:
            await self._websocket.close()

    async def recv(self) -> RelayMessage:
        """Receive the next message.

        Returns:
            The message received from the relay server.

        Raises:
            RelayMessageDecodeError: If the message received cannot
                be decoded into the appropriate message type.
        """
        try:
            websocket = self.websocket
        except RelayNotConnectedError:
            await self.connect()
            websocket = self.websocket

        message_str = await websocket.recv()
        if not isinstance(message_str, str):
            raise AssertionError('Received non-string from websocket.')
        return decode_relay_message(message_str)

    async def send(self, message: RelayMessage) -> None:
        """Send a message.

        Args:
            message: The message to send to the relay server.
        """
        message_str = encode_relay_message(message)

        try:
            websocket = self.websocket
        except RelayNotConnectedError:
            await self.connect()
            websocket = self.websocket

        await websocket.send(message_str)

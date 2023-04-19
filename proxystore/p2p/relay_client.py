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

from proxystore.p2p import messages
from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.utils import hostname

logger = logging.getLogger(__name__)


class RelayServerClient:
    """Client interface to a relay server.

    This interface abstracts the low-level WebSocket connection to a
    relay server to provide automatic reconnection.

    Tip:
        This class can be used as an async context manager!
        ```python
        from proxystore.p2p.relay_client import RelayServerClient

        async with RelayServerClient(...) as client:
            await client.send(...)
            message = await client.recv(...)
        ```

    Note:
        WebSocket connections are not opened until a message is sent,
        a message is received, or
        [`connect()`][proxystore.p2p.relay_client.RelayServerClient.connect]
        is called.

    Args:
        address: Address of the relay server. Should start with `ws://` or
            `wss://`.
        client_uuid: Optional UUID of the client to use when registering with
            the relay server. If `None`, one will be generated.
        client_name: Optional name of the client to use when registering with
            the relay server. If `None`, the hostname will be used.
        timeout: Time to wait in seconds on server connections.
        ssl: When `None`, the correct value to pass to
            [`websockets.connect()`][websockets.client.connect]
            is inferred from `address`. If `address` starts with `wss://` the
            value is True, otherwise is False. Optionally provide a custom
            `SSLContext` (useful if the server uses self-signed certificates).

    Raises:
        PeerRegistrationError: If the connection to the relay server
            is closed, does not reply to the registration request within the
            timeout, or replies with an error.
        ValueError: If address does not start with `ws://` or `wss://`.
    """

    def __init__(
        self,
        address: str,
        client_uuid: uuid.UUID | None = None,
        client_name: str | None = None,
        timeout: float = 10,
        ssl: ssl.SSLContext | None = None,
    ) -> None:
        self.address = address
        self.uuid = uuid.uuid4() if client_uuid is None else client_uuid
        self.name = hostname() if client_name is None else client_name
        self.timeout = timeout

        if not (
            self.address.startswith('ws://')
            or self.address.startswith('wss://')
        ):
            raise ValueError(
                'Relay server address must start with ws:// or wss://.'
                f'Got {self.address}.',
            )
        ssl_default = True if self.address.startswith('wss://') else None
        self.ssl = ssl_default if ssl is None else ssl

        self.initial_backoff_seconds = 1.0

        self._websocket: WebSocketClientProtocol | None = None

    async def __aenter__(self) -> Self:
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
            PeerRegistrationError: If the registration process failed.
        """
        websocket = await websockets.client.connect(
            self.address,
            open_timeout=timeout,
            ssl=self.ssl,
        )

        registration_message = messages.ServerRegistration(
            uuid=self.uuid,
            name=self.name,
        )
        await websocket.send(messages.encode(registration_message))

        try:
            message_str = await asyncio.wait_for(
                websocket.recv(),
                timeout,
            )
            if isinstance(message_str, str):
                message = messages.decode(message_str)
            else:
                raise AssertionError('Received non-string type on websocket.')
        except messages.MessageDecodeError as e:
            raise PeerRegistrationError(
                'Unable to decode response message from relay server.',
            ) from e

        if isinstance(message, messages.ServerResponse):
            if message.success:
                logger.info(
                    'Established client connection to relay server at '
                    f'{self.address} with client uuid={self.uuid} '
                    f'and name={self.name}',
                )
                return websocket
            else:
                raise PeerRegistrationError(
                    'Failed to register as peer with the relay server. '
                    f'Got exception: {message.message}',
                )
        else:
            raise PeerRegistrationError(
                'Relay server replied with unknown message type: '
                f'{type(message).__name__}.',
            )

    async def connect(self) -> WebSocketClientProtocol:
        """Connect to the relay server.

        Note:
            Typically this does not need to be called because the
            send and receive methods will automatically call this.

        Note:
            If an existing and open connection exists, that will be returned.
            Otherwise, a new connection will be attempted with
            exponential backoff for connection failures.

        Returns:
            WebSocket connection to the relay server.
        """
        if self._websocket is not None and self._websocket.open:
            return self._websocket

        backoff_seconds = self.initial_backoff_seconds
        while True:
            try:
                self._websocket = await self._register(timeout=self.timeout)
            except (
                # Exceptions that we should wait and retry again for
                ConnectionRefusedError,
                asyncio.TimeoutError,
                websockets.exceptions.ConnectionClosed,
            ) as e:
                logger.warning(
                    f'Registration with relay server at {self.address} '
                    f'failed because of {e}. Retrying connection in '
                    f'{backoff_seconds} seconds',
                )
                await asyncio.sleep(backoff_seconds)
                backoff_seconds *= 2
            else:
                # Coverage doesn't detect the singular break but it does
                # get executed to break from the loop
                break  # pragma: no cover

        return self._websocket

    async def close(self) -> None:
        """Close the connection to the relay server."""
        if self._websocket is not None:
            await self._websocket.close()

    async def recv(self) -> messages.Message:
        """Receive the next message.

        Returns:
            The message received from the relay server.

        Raises:
            messages.MessageDecodeError: If the message received cannot
                be decoded into the appropriate message type.
        """
        websocket = await self.connect()
        message_str = await websocket.recv()
        if not isinstance(message_str, str):
            raise AssertionError('Received non-string from websocket.')
        return messages.decode(message_str)

    async def send(self, message: messages.Message) -> None:
        """Send a message.

        Args:
            message: The message to send to the relay server.
        """
        message_str = messages.encode(message)
        websocket = await self.connect()
        await websocket.send(message_str)

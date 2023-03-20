"""Functions for connecting to a relay server."""
from __future__ import annotations

import asyncio
import logging
import ssl
from socket import gethostname
from uuid import UUID
from uuid import uuid4

try:
    import websockets.client
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

logger = logging.getLogger(__name__)


async def connect(
    address: str,
    uuid: UUID | None = None,
    name: str | None = None,
    timeout: float = 10,
    ssl: ssl.SSLContext | None = None,
) -> tuple[UUID, str, WebSocketClientProtocol]:
    """Establish client connection to a relay server.

    Args:
        address: Address of the relay server. Should start with ws:// or
            wss://.
        uuid: Optional uuid of client to use when registering with relay
            server.
        name: Readable name of the client to use when registering with the
            relay server. By default the hostname will be used.
        timeout: Time to wait in seconds on server connections.
        ssl: When None, the correct value to pass to
            [`websockets.connect()`][websockets.client.connect]
            is inferred from `address`. If `address` starts with "wss://" the
            value is True, otherwise is False. Optionally provide a custom
            SSLContext (useful if the server uses self-signed certificates).

    Returns:
        Tuple of the UUID of this client returned by the relay server, \
        the name used to register the client, and the websocket connection to \
        the relay server.

    Raises:
        PeerRegistrationError: If the connection to the relay server
            is closed, does not reply to the registration request within the
            timeout, or replies with an error.
        ValueError: If address does not start with "ws://" or "wss://".
    """
    if name is None:
        name = gethostname()
    if uuid is None:
        uuid = uuid4()

    if not (address.startswith('ws://') or address.startswith('wss://')):
        raise ValueError(
            'Relay server address must start with ws:// or wss://.'
            f'Got {address}.',
        )
    ssl_default = True if address.startswith('wss://') else None

    logger.info(
        'Attempting client connection to relay server at '
        f'{address} with uuid={uuid} and name={name} (ssl: {ssl_default})',
    )

    websocket = await websockets.client.connect(
        address,
        open_timeout=timeout,
        ssl=ssl_default if ssl is None else ssl,
    )

    await websocket.send(
        messages.encode(messages.ServerRegistration(uuid=uuid, name=name)),
    )
    try:
        message_str = await asyncio.wait_for(websocket.recv(), timeout)
        if isinstance(message_str, str):
            message = messages.decode(message_str)
        else:
            raise AssertionError('Received non-bytes type on websocket.')
    except websockets.exceptions.ConnectionClosed as e:
        raise PeerRegistrationError(
            'Connection to relay server closed before peer '
            'registration completed.',
        ) from e
    except messages.MessageDecodeError as e:
        raise PeerRegistrationError(
            'Unable to decode response message from relay server.',
        ) from e
    except asyncio.TimeoutError as e:
        raise PeerRegistrationError(
            'Relay server did not reply to registration within timeout.',
        ) from e

    if isinstance(message, messages.ServerResponse):
        if message.success:
            logger.info(
                'Established client connection to relay server at '
                f'{address} with uuid={uuid} and name={name}',
            )
            return uuid, name, websocket
        else:
            raise PeerRegistrationError(
                'Failed to register as peer with relay server. '
                f'Got exception: {message.message}',
            )
    else:
        raise PeerRegistrationError(
            'Relay server replied with unknown message type: '
            f'{type(message).__name__}.',
        )

"""Functions for connecting to the Signaling Server."""
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
    """Establish client connection to a Signaling Server.

    Args:
        address (str): address of the Signaling Server. Should start with
            ws:// or wss://.
        uuid (str, optional): optional uuid of client to use when registering
            with signaling server (default: None).
        name (str, optional): readable name of the client to use when
            registering with the signaling server. By default the
            hostname will be used (default: None).
        timeout (float): time to wait in seconds on server connections
            (default: 10).
        ssl (SSLContext, optional): by default when None, the correct value to
            pass to :code:`websockets.connect` is inferred from `address`.
            If `address` starts with "wss://" the value is True, otherwise is
            False. Optionally provide a custom SSLContext (useful
            if the server uses self-signed certificates) (default: None).

    Returns:
        tuple of the UUID of this client returned by the signaling server,
        the name used to register the client, and the websocket connection to
        the signaling server.

    Raises:
        EndpointRegistrationError:
            if the connection to the signaling server is closed, does not reply
            to the registration request within the timeout, or replies with an
            error.
        ValueError:
            if address does not start with ws:// or wss://.
    """
    if name is None:
        name = gethostname()
    if uuid is None:
        uuid = uuid4()

    if not (address.startswith('ws://') or address.startswith('wss://')):
        raise ValueError(
            'Signaling server address must start with ws:// or wss://.'
            f'Got {address}.',
        )
    ssl_default = True if address.startswith('wss://') else None

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
            'Connection to signaling server closed before peer '
            'registration completed.',
        ) from e
    except messages.MessageDecodeError as e:
        raise PeerRegistrationError(
            'Unable to decode response message from signaling server.',
        ) from e
    except asyncio.TimeoutError as e:
        raise PeerRegistrationError(
            'Signaling server did not reply to registration within timeout.',
        ) from e

    if isinstance(message, messages.ServerResponse):
        if message.success:
            logger.info(
                'established client connection to signaling server at '
                f'{address} with uuid={uuid} and name={name}',
            )
            return uuid, name, websocket
        else:
            raise PeerRegistrationError(
                'Failed to register as peer with signaling server. '
                f'Got exception: {message.message}',
            )
    else:
        raise PeerRegistrationError(
            'Signaling server replied with unknown message type: '
            f'{type(message).__name__}.',
        )

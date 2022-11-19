from __future__ import annotations

import asyncio
import sys
from unittest import mock
from uuid import UUID
from uuid import uuid4

import pytest
import websockets

from proxystore.p2p import messages
from proxystore.p2p.client import connect
from proxystore.p2p.exceptions import PeerRegistrationError

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock

# Use 100ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.1


@pytest.mark.asyncio
async def test_connect_and_ping_server(signaling_server) -> None:
    uuid, name, websocket = await connect(signaling_server.address)
    assert isinstance(uuid, UUID)
    assert isinstance(name, str)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio
async def test_invalid_address_protocol() -> None:
    with pytest.raises(ValueError, match='wss://'):
        await connect('myserver.com', name='test', uuid=uuid4())


@pytest.mark.asyncio
async def test_connect_exceptions(signaling_server) -> None:
    async def sleep(*args, **kwargs) -> None:
        await asyncio.sleep(10)

    # Check timeout on receiving EndpointRegistationSuccess
    with mock.patch('websockets.WebSocketClientProtocol.recv', sleep):
        with pytest.raises(PeerRegistrationError, match='timeout'):
            await connect(signaling_server.address, timeout=_WAIT_FOR)

    # Check error if server returns error
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(
            return_value=messages.encode(
                messages.ServerResponse(
                    success=False,
                    message='test error',
                    error=True,
                ),
            ),
        ),
    ):
        with pytest.raises(PeerRegistrationError, match='test error'):
            await connect(signaling_server.address)

    # Check error if return message from signaling server is unknown type
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(return_value='nonsense message'),
    ):
        with pytest.raises(PeerRegistrationError, match='Unable to decode'):
            await connect(signaling_server.address)

    async def close(*args, **kwargs) -> None:
        raise websockets.exceptions.ConnectionClosedError(None, None)

    # Check connection closed
    with mock.patch('websockets.WebSocketClientProtocol.recv', close):
        with pytest.raises(PeerRegistrationError, match='closed'):
            await connect(signaling_server.address)

    # Unknown response from server
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(
            return_value=messages.encode(
                messages.ServerRegistration('name', uuid4()),
            ),
        ),
    ):
        with pytest.raises(
            PeerRegistrationError,
            match='unknown message type',
        ):
            await connect(signaling_server.address)

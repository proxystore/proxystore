from __future__ import annotations

import asyncio
import logging
import ssl
import uuid
from unittest import mock
from unittest.mock import AsyncMock

import pytest

from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.messages import encode
from proxystore.p2p.messages import ServerRegistration
from proxystore.p2p.messages import ServerResponse
from proxystore.p2p.relay import BasicRelayClient

# Use 100ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.1


def test_invalid_address_protocol() -> None:
    with pytest.raises(ValueError, match='wss://'):
        BasicRelayClient('myserver.com')


@pytest.mark.asyncio()
async def test_default_ssl_context() -> None:
    client = BasicRelayClient('wss://myserver.com', ssl_context=None)
    assert client._ssl_context is not None


@pytest.mark.asyncio()
async def test_default_ssl_context_no_verify() -> None:
    client = BasicRelayClient(
        'wss://myserver.com',
        ssl_context=None,
        verify_certificate=False,
    )
    assert client._ssl_context is not None
    assert client._ssl_context.check_hostname is False
    assert client._ssl_context.verify_mode == ssl.CERT_NONE


@pytest.mark.asyncio()
async def test_open_and_close() -> None:
    client = BasicRelayClient('ws://localhost')
    await client.close()


@pytest.mark.asyncio()
async def test_connect_and_ping_server(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        websocket = await client.connect()
        pong_waiter = await websocket.ping()
        await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio()
async def test_send_recv(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        message = ServerRegistration(name=client.name, uuid=client.uuid)
        await client.send(message)
        response = await asyncio.wait_for(client.recv(), _WAIT_FOR)
        assert isinstance(response, ServerResponse)


@pytest.mark.asyncio()
async def test_recv_wrong_type(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        websocket = await client.connect()
        with mock.patch.object(websocket, 'recv', AsyncMock(return_value=b'')):
            with pytest.raises(AssertionError, match='non-string'):
                await client.recv()


@pytest.mark.asyncio()
async def test_connect_received_non_string(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=b''),
        ):
            with pytest.raises(AssertionError, match='non-string'):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_received_bad_message(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value='bad message'),
        ):
            with pytest.raises(
                PeerRegistrationError,
                match='Unable to decode response message',
            ):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_failure(relay_server) -> None:
    message = ServerResponse(success=False, message='test error', error=True)
    async with BasicRelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=encode(message)),
        ):
            with pytest.raises(PeerRegistrationError, match='test error'):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_unknown_response(relay_server) -> None:
    message = ServerRegistration('name', uuid.uuid4())
    async with BasicRelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=encode(message)),
        ):
            with pytest.raises(
                PeerRegistrationError,
                match='unknown message type',
            ):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_relay_server_backoff(relay_server, caplog) -> None:
    caplog.set_level(logging.WARNING)
    client = BasicRelayClient(relay_server.address, reconnect_task=False)
    client._initial_backoff_seconds = 0.01
    # First and second connection fails but third will work
    with mock.patch.object(
        client,
        '_register',
        AsyncMock(
            side_effect=[asyncio.TimeoutError, asyncio.TimeoutError, None],
        ),
    ):
        await client.connect()

    records = [
        record.message
        for record in caplog.records
        if 'Retrying connection in' in record.message
    ]
    assert len(records) == 2
    assert '0.01 seconds' in records[0]
    assert '0.02 seconds' in records[1]

    await client.close()


@pytest.mark.asyncio()
async def test_relay_server_manual_reconnection(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        websocket = await client.connect()
        await websocket.close()
        # We should get a new connection now that we closed the old one
        assert websocket != await client.connect()


@pytest.mark.asyncio()
async def test_relay_server_auto_reconnection(relay_server) -> None:
    async with BasicRelayClient(relay_server.address) as client:
        websocket = await client.connect()
        await websocket.close()
        assert client._websocket is not None
        assert client._websocket.closed
        # Give opportunity to yield control to any clean up methods within
        # the websocket.
        for _ in range(10):
            await asyncio.sleep(0.001)
        assert not client._websocket.closed
        assert client._websocket != websocket

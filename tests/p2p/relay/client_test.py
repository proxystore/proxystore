from __future__ import annotations

import asyncio
import logging
import ssl
import uuid
from unittest import mock
from unittest.mock import AsyncMock

import pytest

from proxystore.p2p.relay.client import RelayClient
from proxystore.p2p.relay.exceptions import RelayNotConnectedError
from proxystore.p2p.relay.exceptions import RelayRegistrationError
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse

# Use 100ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.1


def test_invalid_address_protocol() -> None:
    with pytest.raises(ValueError, match='wss://'):
        RelayClient('myserver.com')


@pytest.mark.asyncio()
async def test_default_ssl_context() -> None:
    client = RelayClient('wss://myserver.com', ssl_context=None)
    assert client._ssl_context is not None


@pytest.mark.asyncio()
async def test_default_ssl_context_no_verify() -> None:
    client = RelayClient(
        'wss://myserver.com',
        ssl_context=None,
        verify_certificate=False,
    )
    assert client._ssl_context is not None
    assert client._ssl_context.check_hostname is False
    assert client._ssl_context.verify_mode == ssl.CERT_NONE


@pytest.mark.asyncio()
async def test_open_and_close() -> None:
    client = RelayClient('ws://localhost')
    await client.close()


@pytest.mark.asyncio()
async def test_connect_and_ping_server(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        pong_waiter = await client.websocket.ping()
        await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio()
async def test_send_recv(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        message = RelayRegistrationRequest(name=client.name, uuid=client.uuid)
        await client.send(message)
        response = await asyncio.wait_for(client.recv(), _WAIT_FOR)
        assert isinstance(response, RelayResponse)


@pytest.mark.asyncio()
async def test_recv_wrong_type(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        with mock.patch.object(
            client.websocket,
            'recv',
            AsyncMock(return_value=b''),
        ):
            with pytest.raises(AssertionError, match='non-string'):
                await client.recv()


@pytest.mark.asyncio()
async def test_connect_received_non_string(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=b''),
        ):
            with pytest.raises(AssertionError, match='non-string'):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_received_bad_message(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value='bad message'),
        ):
            with pytest.raises(
                RelayRegistrationError,
                match='Unable to decode response message',
            ):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_failure(relay_server) -> None:
    message = RelayResponse(success=False, message='test error', error=True)
    async with RelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=encode_relay_message(message)),
        ):
            with pytest.raises(RelayRegistrationError, match='test error'):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_unknown_response(relay_server) -> None:
    message = RelayRegistrationRequest('name', uuid.uuid4())
    async with RelayClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=encode_relay_message(message)),
        ):
            with pytest.raises(
                RelayRegistrationError,
                match='unknown message type',
            ):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_relay_server_backoff(relay_server, caplog) -> None:
    caplog.set_level(logging.WARNING)
    client = RelayClient(relay_server.address, reconnect_task=False)
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
async def test_connect_on_send(relay_server) -> None:
    client = RelayClient(relay_server.address)
    with pytest.raises(RelayNotConnectedError):
        assert client.websocket is None
    message = PeerConnectionRequest(
        client.uuid,
        client.name,
        uuid.uuid4(),
        'offer',
        'test',
    )
    await client.send(message)
    assert client.websocket is not None
    await client.recv()
    await client.close()


@pytest.mark.asyncio()
async def test_connect_on_recv(relay_server) -> None:
    client = RelayClient(relay_server.address)
    with pytest.raises(RelayNotConnectedError):
        assert client.websocket is None
    with mock.patch.object(client, 'connect', AsyncMock()) as mock_connect:
        with pytest.raises(RelayNotConnectedError):
            # This will fail once it tries to get the open websocket
            # because we mocked connect, but we just want to make sure
            # connect is called
            await client.recv()
        mock_connect.assert_awaited_once()
    await client.close()


@pytest.mark.asyncio()
async def test_relay_server_manual_reconnection(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        old_websocket = client.websocket
        await old_websocket.close()
        # We should get a new connection now that we closed the old one
        await client.connect()
        assert client.websocket != old_websocket


@pytest.mark.asyncio()
async def test_relay_server_auto_reconnection(relay_server) -> None:
    async with RelayClient(relay_server.address) as client:
        old_websocket = client.websocket
        await old_websocket.close()
        assert old_websocket.closed
        # Give opportunity to yield control to any clean up methods within
        # the websocket.
        for _ in range(10):
            await asyncio.sleep(0.001)
        assert not client.websocket.closed
        assert client.websocket != old_websocket

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from unittest import mock

import pytest

from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.messages import encode
from proxystore.p2p.messages import ServerRegistration
from proxystore.p2p.messages import ServerResponse
from proxystore.p2p.relay_client import RelayServerClient

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock

# Use 100ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.1


def test_invalid_address_protocol() -> None:
    with pytest.raises(ValueError, match='wss://'):
        RelayServerClient('myserver.com')


@pytest.mark.asyncio()
async def test_open_and_close() -> None:
    client = RelayServerClient('ws://localhost')
    await client.close()


@pytest.mark.asyncio()
async def test_connect_and_ping_server(relay_server) -> None:
    async with RelayServerClient(relay_server.address) as client:
        websocket = await client.connect()
        pong_waiter = await websocket.ping()
        await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio()
async def test_send_recv(relay_server) -> None:
    async with RelayServerClient(relay_server.address) as client:
        message = ServerRegistration(name=client.name, uuid=client.uuid)
        await client.send(message)
        response = await asyncio.wait_for(client.recv(), _WAIT_FOR)
        assert isinstance(response, ServerResponse)


@pytest.mark.asyncio()
async def test_recv_wrong_type(relay_server) -> None:
    async with RelayServerClient(relay_server.address) as client:
        websocket = await client.connect()
        with mock.patch.object(websocket, 'recv', AsyncMock(return_value=b'')):
            with pytest.raises(AssertionError, match='non-string'):
                await client.recv()


@pytest.mark.asyncio()
async def test_connect_received_non_string(relay_server) -> None:
    async with RelayServerClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=b''),
        ):
            with pytest.raises(AssertionError, match='non-string'):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_received_bad_message(relay_server) -> None:
    async with RelayServerClient(relay_server.address) as client:
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
    async with RelayServerClient(relay_server.address) as client:
        with mock.patch(
            'websockets.WebSocketClientProtocol.recv',
            AsyncMock(return_value=encode(message)),
        ):
            with pytest.raises(PeerRegistrationError, match='test error'):
                await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_unknown_response(relay_server) -> None:
    message = ServerRegistration('name', uuid.uuid4())
    async with RelayServerClient(relay_server.address) as client:
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
    async with RelayServerClient(relay_server.address) as client:
        client.initial_backoff_seconds = 0.01
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


@pytest.mark.asyncio()
async def test_relay_server_reconnection(relay_server) -> None:
    async with RelayServerClient(relay_server.address) as client:
        websocket = await client.connect()
        await websocket.close()
        # We should get a new connection now that we closed the old one
        assert websocket != await client.connect()

from __future__ import annotations

import asyncio
import logging
import ssl
import uuid
from typing import AsyncGenerator
from typing import Generator
from unittest import mock
from unittest.mock import AsyncMock

import globus_sdk
import pytest
import pytest_asyncio

from proxystore.p2p.relay.exceptions import RelayNotConnectedError
from proxystore.p2p.relay.exceptions import RelayRegistrationError
from proxystore.p2p.relay.globus.client import GlobusAuthRelayClient
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse
from tests.p2p.relay.globus.conftest import RelayServerInfo

# Use 100ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.1


@pytest.fixture()
def mock_authorizer() -> Generator[globus_sdk.RenewingAuthorizer, None, None]:
    with mock.patch('globus_sdk.authorizers.RefreshTokenAuthorizer'):
        authorizer = globus_sdk.authorizers.RefreshTokenAuthorizer(
            '<TOKEN>',
            None,  # type: ignore[arg-type]
        )
        with mock.patch.object(
            authorizer,
            'get_authorization_header',
            return_value='Bearer <TOKEN>',
        ):
            yield authorizer


@pytest_asyncio.fixture()
@pytest.mark.asyncio()
async def client(
    globus_auth_relay: RelayServerInfo,
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> AsyncGenerator[GlobusAuthRelayClient, None]:
    async with GlobusAuthRelayClient(
        globus_auth_relay.address,
        mock_authorizer,
        verify_certificate=False,
    ) as client:
        yield client


def test_invalid_address_protocol(
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> None:
    with pytest.raises(ValueError, match='wss://'):
        GlobusAuthRelayClient('ws://myserver.com', mock_authorizer)


@pytest.mark.asyncio()
async def test_default_ssl_context(
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> None:
    client = GlobusAuthRelayClient(
        'wss://myserver.com',
        mock_authorizer,
        ssl_context=None,
    )
    assert client._ssl_context is not None


@pytest.mark.asyncio()
async def test_default_ssl_context_no_verify(
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> None:
    client = GlobusAuthRelayClient(
        'wss://myserver.com',
        mock_authorizer,
        ssl_context=ssl.create_default_context(),
        verify_certificate=False,
    )
    assert client._ssl_context is not None
    assert client._ssl_context.check_hostname is False
    assert client._ssl_context.verify_mode == ssl.CERT_NONE


@pytest.mark.asyncio()
async def test_open_and_close(
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> None:
    client = GlobusAuthRelayClient('wss://localhost', mock_authorizer)
    await client.close()


@pytest.mark.asyncio()
async def test_connect_and_ping_server(client: GlobusAuthRelayClient) -> None:
    pong_waiter = await client.websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio()
async def test_recv_wrong_type(client: GlobusAuthRelayClient) -> None:
    with mock.patch.object(
        client.websocket,
        'recv',
        AsyncMock(return_value=b''),
    ):
        with pytest.raises(AssertionError, match='non-string'):
            await client.recv()


@pytest.mark.asyncio()
async def test_connect_received_non_string(
    client: GlobusAuthRelayClient,
) -> None:
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(return_value=b''),
    ):
        with pytest.raises(AssertionError, match='non-string'):
            await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_received_bad_message(
    client: GlobusAuthRelayClient,
) -> None:
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
async def test_connect_failure(
    client: GlobusAuthRelayClient,
) -> None:
    message = RelayResponse(success=False, message='test error', error=True)
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(return_value=encode_relay_message(message)),
    ):
        with pytest.raises(RelayRegistrationError, match='test error'):
            await client._register(_WAIT_FOR)


@pytest.mark.asyncio()
async def test_connect_unknown_response(
    client: GlobusAuthRelayClient,
) -> None:
    message = RelayRegistrationRequest('name', uuid.uuid4())
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
async def test_globus_auth_relay_backoff(
    globus_auth_relay: RelayServerInfo,
    mock_authorizer: globus_sdk.RenewingAuthorizer,
    caplog,
) -> None:
    caplog.set_level(logging.WARNING)
    client = GlobusAuthRelayClient(
        globus_auth_relay.address,
        mock_authorizer,
        reconnect_task=False,
        verify_certificate=False,
    )
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
async def test_connect_on_send(
    globus_auth_relay: RelayServerInfo,
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> None:
    client = GlobusAuthRelayClient(
        globus_auth_relay.address,
        mock_authorizer,
        verify_certificate=False,
    )
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
async def test_connect_on_recv(
    globus_auth_relay: RelayServerInfo,
    mock_authorizer: globus_sdk.RenewingAuthorizer,
) -> None:
    client = GlobusAuthRelayClient(
        globus_auth_relay.address,
        mock_authorizer,
        verify_certificate=False,
    )
    with pytest.raises(RelayNotConnectedError):
        assert client.websocket is None
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.recv(), 0.01)
    assert client.websocket is not None
    await client.close()


@pytest.mark.asyncio()
async def test_globus_auth_relay_manual_reconnection(
    client: GlobusAuthRelayClient,
) -> None:
    old_websocket = client.websocket
    await old_websocket.close()
    # We should get a new connection now that we closed the old one
    await client.connect()
    assert client.websocket != old_websocket


@pytest.mark.asyncio()
async def test_globus_auth_relay_auto_reconnection(
    client: GlobusAuthRelayClient,
) -> None:
    old_websocket = client.websocket
    await old_websocket.close()
    assert old_websocket.closed
    # Give opportunity to yield control to any clean up methods within
    # the websocket.
    for _ in range(10):
        await asyncio.sleep(0.001)
    assert not client.websocket.closed
    assert client.websocket != old_websocket

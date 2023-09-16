"""Test fixtures for tests.p2p.relay.globus."""
from __future__ import annotations

import ssl
import uuid
from typing import AsyncGenerator
from typing import NamedTuple
from unittest import mock

import pytest
import pytest_asyncio
import websockets
from websockets.server import WebSocketServer

from proxystore.globus.client import get_confidential_app_auth_client
from proxystore.p2p.relay.globus.server import GlobusAuthRelayServer
from testing.utils import open_port


class RelayServerInfo(NamedTuple):
    """NamedTuple returned by globus_auth_relay fixture."""

    relay_server: GlobusAuthRelayServer
    websocket_server: WebSocketServer
    host: str
    port: int
    address: str
    mock_authenticate_user: mock.MagicMock


@pytest_asyncio.fixture()
@pytest.mark.asyncio()
async def globus_auth_relay(
    ssl_context: ssl.SSLContext,
) -> AsyncGenerator[RelayServerInfo, None]:
    """Run a GlobusAuthRelayServer."""
    host = 'localhost'
    port = open_port()
    address = f'wss://{host}:{port}'

    auth_client = get_confidential_app_auth_client(str(uuid.uuid4()), 'secret')
    server = GlobusAuthRelayServer(auth_client)

    with mock.patch(
        'proxystore.p2p.relay.globus.server.authenticate_user_with_token',
    ) as mock_authenticate_user:
        async with websockets.server.serve(
            server.handler,
            host,
            port,
            ssl=ssl_context,
        ) as websocket_server:
            server_info = RelayServerInfo(
                relay_server=server,
                websocket_server=websocket_server,
                host=host,
                port=port,
                address=address,
                mock_authenticate_user=mock_authenticate_user,
            )
            assert websocket_server.is_serving()
            yield server_info

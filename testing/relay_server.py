"""Tools for running a local relay server for unit tests."""
from __future__ import annotations

from typing import AsyncGenerator
from typing import NamedTuple

import pytest
import pytest_asyncio
import websockets
from websockets.server import WebSocketServer

from proxystore.p2p.relay import RelayServer
from testing.utils import open_port


class RelayServerInfo(NamedTuple):
    """NamedTuple returned by relay_server fixture."""

    relay_server: RelayServer
    websocket_server: WebSocketServer
    host: str
    port: int
    address: str


@pytest_asyncio.fixture(scope='session')
@pytest.mark.asyncio()
async def relay_server(
    event_loop,
) -> AsyncGenerator[RelayServerInfo, None]:
    """Fixture that runs relay server locally.

    Warning:
        This fixture has session scope so the relay server will be shared
        between many tests.

    Yields:
        `RelayServerInfo <.RelayServerInfo>`
    """
    host = 'localhost'
    port = open_port()
    address = f'ws://{host}:{port}'

    relay_server = RelayServer()
    async with websockets.server.serve(
        relay_server.handler,
        host,
        port,
    ) as websocket_server:
        server_info = RelayServerInfo(
            relay_server=relay_server,
            websocket_server=websocket_server,
            host=host,
            port=port,
            address=address,
        )
        assert websocket_server.is_serving()
        yield server_info

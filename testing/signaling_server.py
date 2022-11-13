"""Tools for running a local signaling server for unit tests."""
from __future__ import annotations

from typing import AsyncGenerator
from typing import NamedTuple

import pytest
import pytest_asyncio
import websockets
from websockets.server import WebSocketServer

from proxystore.p2p.server import SignalingServer
from testing.utils import open_port

_SERVER_HOST = 'localhost'
_SERVER_PORT = open_port()
_SERVER_ADDRESS = f'ws://{_SERVER_HOST}:{_SERVER_PORT}'


class SignalingServerInfo(NamedTuple):
    """NamedTuple returned by signaling_server fixture."""

    signaling_server: SignalingServer
    websocket_server: WebSocketServer
    host: str
    port: int
    address: str


@pytest_asyncio.fixture(scope='session')
@pytest.mark.asyncio
async def signaling_server(
    event_loop,
) -> AsyncGenerator[SignalingServerInfo, None]:
    """Fixture that runs signaling server locally.

    Warning:
        This fixture has session scope so the signaling server will be shared
        between many tests.

    Yields:
        `SignalingServerInfo <.SignalingServerInfo>`
    """
    signaling_server = SignalingServer()
    async with websockets.server.serve(
        signaling_server.handler,
        _SERVER_HOST,
        _SERVER_PORT,
    ) as websocket_server:
        server_info = SignalingServerInfo(
            signaling_server=signaling_server,
            websocket_server=websocket_server,
            host=_SERVER_HOST,
            port=_SERVER_PORT,
            address=_SERVER_ADDRESS,
        )
        assert websocket_server.is_serving()
        yield server_info

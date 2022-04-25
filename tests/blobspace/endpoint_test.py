from __future__ import annotations

import pytest
import pytest_asyncio
import websockets

from proxystore.blobspace.endpoint import Endpoint
from proxystore.blobspace.server import SignalingServer

SERVER_HOST = 'localhost'
SERVER_PORT = 8765
SERVER_ADDRESS = f'{SERVER_HOST}:{SERVER_PORT}'


@pytest_asyncio.fixture
@pytest.mark.asyncio
async def server():
    signaling_server = SignalingServer()
    async with websockets.serve(
        signaling_server.handler,
        SERVER_HOST,
        SERVER_PORT,
    ) as server:
        assert server.is_serving()
        yield signaling_server


@pytest.mark.asyncio
async def test_endpoint_init(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'

    endpoint1 = await Endpoint(
        uuid=peer1,
        name=peer1_name,
        signaling_server_address=SERVER_ADDRESS,
    )

    await endpoint1.close()


@pytest.mark.asyncio
async def test_endpoint_p2p_connection(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    peer2 = 'fake-uuid-2'
    peer2_name = 'test-client-2'

    endpoint1 = await Endpoint(
        uuid=peer1,
        name=peer1_name,
        signaling_server_address=SERVER_ADDRESS,
    )
    endpoint2 = await Endpoint(
        uuid=peer2,
        name=peer2_name,
        signaling_server_address=SERVER_ADDRESS,
    )

    await endpoint1._connect_to_peer(peer2)

    await endpoint1.close()
    await endpoint2.close()

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass

import pytest
import pytest_asyncio
import websockets

from proxystore.blobspace.messages import BaseMessage
from proxystore.blobspace.messages import P2PConnectionError
from proxystore.blobspace.messages import P2PDataTransfer
from proxystore.blobspace.p2p import P2PConnection
from proxystore.blobspace.p2p import P2PConnectionManager
from proxystore.blobspace.server import connect
from proxystore.blobspace.server import SignalingServer
from proxystore.serialize import serialize

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asyncmock import AsyncMock


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
async def test_p2p_manager_awaitable(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )
    manager = await P2PConnectionManager(peer1, peer1_name, websocket1)
    await manager.close()


@pytest.mark.asyncio
async def test_p2p_connection(server) -> None:
    peer1 = 'fake-uuid-1'
    peer2 = 'fake-uuid-2'
    peer1_name = 'test-client-1'
    peer2_name = 'test-client-2'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )
    websocket2 = await connect(
        uuid=peer2,
        name=peer2_name,
        address=SERVER_ADDRESS,
    )
    async with P2PConnectionManager(
        peer1,
        peer1_name,
        websocket1,
    ) as manager1, P2PConnectionManager(
        peer2,
        peer2_name,
        websocket2,
    ) as manager2:
        connection1 = await manager1.get_connection(peer2)
        assert connection1 == await manager1.new_connection(peer2)
        await connection1.wait()
        assert connection1.state == 'connected'

        connection2 = await manager2.get_connection(peer1)
        assert connection2.state == 'connected'
        await connection2.wait()

    await websocket1.close()
    await websocket2.close()


@pytest.mark.asyncio
async def test_p2p_message_passing(server) -> None:
    peer1 = 'fake-uuid-1'
    peer2 = 'fake-uuid-2'
    peer1_name = 'test-client-1'
    peer2_name = 'test-client-2'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )
    websocket2 = await connect(
        uuid=peer2,
        name=peer2_name,
        address=SERVER_ADDRESS,
    )

    @dataclass
    class P2PTestMessage(P2PDataTransfer):

        message: str

    async with P2PConnectionManager(
        peer1,
        peer1_name,
        websocket1,
    ) as manager1, P2PConnectionManager(
        peer2,
        peer2_name,
        websocket2,
    ) as manager2:
        connection1 = await manager1.get_connection(peer2)
        await connection1.wait()
        connection2 = await manager2.get_connection(peer1)
        assert connection2.state == 'connected'

        await connection1.send(P2PTestMessage('hello'))
        message1 = await connection2.recv()
        assert isinstance(message1, P2PTestMessage)
        assert message1.message == 'hello'

        await connection2.send(P2PTestMessage('hello hello'))
        message1 = await connection1.recv()
        assert isinstance(message1, P2PTestMessage)
        assert message1.message == 'hello hello'

    await websocket1.close()
    await websocket2.close()


@pytest.mark.asyncio
async def test_p2p_connection_error(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )
    connection = P2PConnection(peer1, websocket1)
    with pytest.raises(RuntimeError, match='failed to connect to peer'):
        await connection.handle_message(
            P2PConnectionError(
                source_uuid='',
                target_uuid=peer1,
                error='test',
            ),
        )

    await websocket1.close()


@pytest.mark.asyncio
async def test_expected_server_disconnect(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )
    manager = await P2PConnectionManager(peer1, peer1_name, websocket1)
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await websocket1.close()
    await manager.close()


@pytest.mark.asyncio
async def test_unexpected_server_disconnect(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )
    manager = await P2PConnectionManager(peer1, peer1_name, websocket1)
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await server._uuid_to_client[peer1].websocket.close(code=1002)
    await manager.close()


@pytest.mark.asyncio
async def test_serialization_error(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )

    # P2PConnectionManager should log an error and skip the message but
    # not raise an exception. Note we use side_effect here so recv() only
    # happens once
    websocket1.recv = AsyncMock(side_effect=[b'nonsense_string'])
    async with P2PConnectionManager(peer1, peer1_name, websocket1):
        # Give time for message_handler task to run once and read message
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_unknown_message_type(server) -> None:
    peer1 = 'fake-uuid-1'
    peer1_name = 'test-client-1'
    websocket1 = await connect(
        uuid=peer1,
        name=peer1_name,
        address=SERVER_ADDRESS,
    )

    # P2PConnectionManager should log an error and skip the message but
    # not raise an exception. Note we use side_effect here so recv() only
    # happens once
    websocket1.recv = AsyncMock(side_effect=[serialize(BaseMessage())])
    async with P2PConnectionManager(peer1, peer1_name, websocket1):
        # Give time for message_handler task to run once and read message
        await asyncio.sleep(0.1)

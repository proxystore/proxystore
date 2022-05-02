from __future__ import annotations

import asyncio
import logging

import pytest

from proxystore.p2p.manager import PeerManager
from proxystore.p2p.server import connect
from proxystore.serialize import serialize
from testing.mocking import async_mock_once


@pytest.mark.asyncio
async def test_p2p_manager_awaitable(signaling_server) -> None:
    uuid, websocket = await connect(signaling_server.address)
    manager = await PeerManager(uuid, websocket)
    await manager.close()


@pytest.mark.asyncio
async def test_p2p_connection(signaling_server) -> None:
    peer1, websocket1 = await connect(signaling_server.address)
    peer2, websocket2 = await connect(signaling_server.address)

    async with PeerManager(peer1, websocket1) as manager1, PeerManager(
        peer2,
        websocket2,
    ) as manager2:
        connection1 = await manager1.get_connection(peer2)
        assert connection1 == await manager1.get_connection(peer2)
        await connection1.wait()
        assert connection1.state == 'connected'

        connection2 = await manager2.get_connection(peer1)
        await connection2.wait()
        assert connection2.state == 'connected'

    await websocket1.close()
    await websocket2.close()


@pytest.mark.asyncio
async def test_p2p_messaging(signaling_server) -> None:
    peer1, websocket1 = await connect(signaling_server.address)
    peer2, websocket2 = await connect(signaling_server.address)

    async with PeerManager(peer1, websocket1) as manager1, PeerManager(
        peer2,
        websocket2,
    ) as manager2:
        await manager1.send(peer2, 'hello hello')
        source_uuid, message = await manager2.recv()
        assert source_uuid == peer1
        assert message == 'hello hello'

    await websocket1.close()
    await websocket2.close()


@pytest.mark.asyncio
async def test_expected_server_disconnect(signaling_server) -> None:
    peer1, websocket1 = await connect(signaling_server.address)
    manager = await PeerManager(peer1, websocket1)
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await websocket1.close()
    await manager.close()


@pytest.mark.asyncio
async def test_unexpected_server_disconnect(signaling_server) -> None:
    peer1, websocket1 = await connect(signaling_server.address)
    manager = await PeerManager(peer1, websocket1)
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await signaling_server.signaling_server._uuid_to_client[
        peer1
    ].websocket.close(code=1002)
    await manager.close()


@pytest.mark.asyncio
async def test_serialization_error(signaling_server, caplog) -> None:
    peer1, websocket1 = await connect(signaling_server.address)
    # PeerManager should log an error and skip the message but
    # not raise an exception.
    websocket1.recv = async_mock_once(websocket1.recv, b'nonsense_string')
    caplog.set_level(logging.ERROR)
    async with PeerManager(peer1, websocket1):
        while not websocket1.recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'deserialization error' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio
async def test_unknown_message_type(signaling_server, caplog) -> None:
    peer1, websocket1 = await connect(signaling_server.address)
    # PeerManager should log an error and skip the message but
    # not raise an exception.
    websocket1.recv = async_mock_once(
        websocket1.recv,
        serialize('random message'),
    )
    caplog.set_level(logging.ERROR)
    async with PeerManager(peer1, websocket1):
        while not websocket1.recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'unknown message' in record.message and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )

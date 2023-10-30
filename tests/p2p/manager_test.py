from __future__ import annotations

import asyncio
import logging
import uuid

import pytest

from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.relay.client import RelayClient
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse
from testing.mocking import async_mock_once


@pytest.mark.asyncio()
async def test_awaitable(relay_server) -> None:
    manager = await PeerManager(RelayClient(relay_server.address))
    # Calling async_init again should do nothing
    await manager.async_init()
    await manager.close()


@pytest.mark.asyncio()
async def test_not_awaited(relay_server) -> None:
    manager = PeerManager(RelayClient(relay_server.address))
    with pytest.raises(RuntimeError, match='await'):
        await manager.get_connection(uuid.uuid4())
    with pytest.raises(RuntimeError, match='await'):
        await manager.close()


@pytest.mark.asyncio()
async def test_uuid_name_properties(relay_server) -> None:
    uuid_ = uuid.uuid4()
    name = 'pm'
    relay_client = RelayClient(
        relay_server.address,
        client_name=name,
        client_uuid=uuid_,
    )
    async with PeerManager(relay_client) as manager:
        assert manager.uuid == uuid_
        assert manager.name == name


@pytest.mark.asyncio()
async def test_p2p_connection(relay_server) -> None:
    async with PeerManager(
        RelayClient(relay_server.address),
    ) as manager1, PeerManager(
        RelayClient(relay_server.address),
    ) as manager2:
        connection1 = await manager1.get_connection(manager2.uuid)
        assert connection1 == await manager1.get_connection(manager2.uuid)
        await connection1.ready()
        assert connection1.state == 'connected'

        connection2 = await manager2.get_connection(manager1.uuid)
        await connection2.ready()
        assert connection2.state == 'connected'


@pytest.mark.asyncio()
async def test_p2p_connection_error_unknown_peer(relay_server) -> None:
    relay_client = RelayClient(relay_server.address)
    async with PeerManager(relay_client) as manager:
        with pytest.raises(
            PeerConnectionError,
            match='Cannot forward peer connection message',
        ):
            await manager.send(uuid.uuid4(), 'hello', timeout=0.2)


@pytest.mark.asyncio()
async def test_p2p_connection_error_from_server(relay_server) -> None:
    # Record current tasks so we know which not to clean up
    task_names = {task.get_name() for task in asyncio.all_tasks()}

    async with PeerManager(
        RelayClient(relay_server.address),
    ) as manager1, PeerManager(
        RelayClient(relay_server.address),
    ) as manager2:
        # Mock manager 1 to receive error peer connection from relay server
        mock_recv = async_mock_once(
            manager1._relay_client.recv,
            PeerConnectionRequest(
                source_uuid=manager2.uuid,
                source_name=manager2.name,
                peer_uuid=manager1.uuid,
                description_type='offer',
                description='',
                error='test error',
            ),
        )
        manager1._relay_client.recv = mock_recv  # type: ignore

        connection1 = await manager1.get_connection(manager2.uuid)

        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

        with pytest.raises(PeerConnectionError, match='test error'):
            await connection1.ready()

    # Clean up tasks that were left pending because we raised an exception
    for task in asyncio.all_tasks():
        if task.get_name() not in task_names:
            task.cancel()
            try:
                await task
            # For note on AttributeError catching:
            # https://github.com/proxystore/proxystore/issues/405
            except (asyncio.CancelledError, AttributeError):
                pass


@pytest.mark.asyncio()
async def test_p2p_messaging(relay_server) -> None:
    async with PeerManager(
        RelayClient(relay_server.address),
    ) as manager1, PeerManager(
        RelayClient(relay_server.address),
    ) as manager2:
        await manager1.send(manager2.uuid, 'hello hello')
        source_uuid, message = await manager2.recv()
        assert source_uuid == manager1.uuid
        assert message == 'hello hello'


@pytest.mark.asyncio()
async def test_expected_server_disconnect(relay_server) -> None:
    manager = await PeerManager(RelayClient(relay_server.address))
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await relay_server.relay_server.client_manager.get_client_by_uuid(
        manager.uuid,
    ).websocket.close()
    await manager.close()


@pytest.mark.asyncio()
async def test_unexpected_server_disconnect(relay_server) -> None:
    manager = await PeerManager(RelayClient(relay_server.address))
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await relay_server.relay_server.client_manager.get_client_by_uuid(
        manager.uuid,
    ).websocket.close(code=1002)
    await manager.close()


@pytest.mark.asyncio()
async def test_serialization_error(relay_server, caplog) -> None:
    # PeerManager should log an error and skip the message but
    # not raise an exception.
    caplog.set_level(logging.ERROR)
    relay_client = RelayClient(relay_server.address)
    async with PeerManager(relay_client):
        assert relay_client._websocket is not None
        mock_recv = async_mock_once(
            relay_client._websocket.recv,
            'nonsense_string',
        )
        relay_client._websocket.recv = mock_recv  # type: ignore
        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'error deserializing' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_unexpected_server_response(relay_server, caplog) -> None:
    # PeerManager should log an exception and skip the message but
    # not raise an exception.
    caplog.set_level(logging.ERROR)
    async with PeerManager(RelayClient(relay_server.address)) as manager:
        message = RelayResponse(success=True, message='', error=False)
        mock_recv = async_mock_once(manager._relay_client.recv, message)
        manager._relay_client.recv = mock_recv  # type: ignore
        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'got unexpected' in record.message and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_unknown_message_type(relay_server, caplog) -> None:
    # PeerManager should log an error and skip the message but
    # not raise an exception.
    caplog.set_level(logging.ERROR)
    async with PeerManager(RelayClient(relay_server.address)) as manager:
        message = RelayRegistrationRequest('name', uuid.uuid4())
        mock_recv = async_mock_once(manager._relay_client.recv, message)
        manager._relay_client.recv = mock_recv  # type: ignore
        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'unknown message' in record.message and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_close_connection(relay_server) -> None:
    async with PeerManager(
        RelayClient(relay_server.address),
    ) as manager1, PeerManager(
        RelayClient(relay_server.address),
    ) as manager2:
        # Send message to make sure connection is open
        await manager1.send(manager2.uuid, 'hello hello')
        source_uuid, message = await manager2.recv()
        assert source_uuid == manager1.uuid
        assert message == 'hello hello'

        await manager1.get_connection(manager2.uuid)
        await manager2.get_connection(manager1.uuid)

        await manager1.close_connection((manager1.uuid, manager2.uuid))
        # Should be idempotent
        await manager1.close_connection((manager1.uuid, manager2.uuid))

        # Yield event loop to make sure peer connection closed callbacks fire
        await asyncio.sleep(0.001)

        assert len(manager1._peers) == 0
        assert len(manager2._peers) == 0

        # Send another message to make sure connection is reopened
        await manager1.send(manager2.uuid, 'hello hello again')
        source_uuid, message = await manager2.recv()
        assert source_uuid == manager1.uuid
        assert message == 'hello hello again'

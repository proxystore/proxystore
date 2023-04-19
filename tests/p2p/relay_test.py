from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

import pytest
import websockets

from proxystore.p2p import messages
from proxystore.p2p.relay_client import RelayServerClient

# Use 200ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.2


@pytest.mark.asyncio()
async def test_connect_twice(relay_server) -> None:
    client = RelayServerClient(relay_server.address)
    websocket = await client.connect()
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)
    await websocket.send(
        messages.encode(
            messages.ServerRegistration(
                name='different-host',
                uuid=client.uuid,
            ),
        ),
    )
    message_ = await asyncio.wait_for(websocket.recv(), _WAIT_FOR)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.ServerResponse)
    assert message.success
    assert not message.error


@pytest.mark.asyncio()
async def test_connect_reconnect_new_socket(relay_server) -> None:
    client1 = RelayServerClient(relay_server.address)
    websocket1 = await client1.connect()
    pong_waiter = await websocket1.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)

    client2 = RelayServerClient(
        relay_server.address,
        client_uuid=client1.uuid,
        client_name=client1.name,
    )
    websocket2 = await client2.connect()
    assert client1.uuid == client2.uuid
    await websocket1.wait_closed()
    assert websocket1.close_code != 1000
    pong_waiter = await websocket2.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio()
async def test_expected_client_disconnect(relay_server) -> None:
    client = RelayServerClient(relay_server.address)
    websocket = await client.connect()
    server_client = relay_server.relay_server._uuid_to_client[client.uuid]
    assert (
        server_client
        in relay_server.relay_server._websocket_to_client.values()
    )

    await websocket.close()
    # TODO(gpauloski): remove sleep. It is here to give time for the
    # server's unregister coroutine to finish
    await asyncio.sleep(0.05)

    assert client.uuid not in relay_server.relay_server._uuid_to_client
    assert (
        server_client
        not in relay_server.relay_server._websocket_to_client.values()
    )


@pytest.mark.asyncio()
async def test_unexpected_client_disconnect(relay_server) -> None:
    client = RelayServerClient(relay_server.address)
    websocket = await client.connect()
    server_client = relay_server.relay_server._uuid_to_client[client.uuid]
    assert (
        server_client
        in relay_server.relay_server._websocket_to_client.values()
    )

    await websocket.close(code=1002)
    # TODO(gpauloski): remove sleep. It is here to give time for the
    # server's unregister coroutine to finish
    await asyncio.sleep(0.05)

    assert client.uuid not in relay_server.relay_server._uuid_to_client
    assert (
        server_client
        not in relay_server.relay_server._websocket_to_client.values()
    )


@pytest.mark.asyncio()
async def test_server_deserialization_fails_silently(relay_server) -> None:
    client = RelayServerClient(relay_server.address)
    websocket = await client.connect()
    # This message should cause deserialization error on server but
    # server should catch and wait for next message
    await websocket.send('invalid message')
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio()
async def test_endpoint_not_registered_error(relay_server) -> None:
    websocket = await websockets.client.connect(relay_server.address)
    await websocket.send(
        messages.encode(
            messages.PeerConnection(
                source_uuid=uuid4(),
                source_name='',
                peer_uuid=uuid4(),
                description_type='offer',
                description='',
            ),
        ),
    )
    message_ = await asyncio.wait_for(websocket.recv(), 1)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.ServerResponse)
    assert message.error
    assert message.message is not None
    assert 'not registered' in message.message


@pytest.mark.asyncio()
async def test_p2p_message_passing(relay_server) -> None:
    client1 = RelayServerClient(relay_server.address)
    await client1.connect()
    peer1_name, peer1_uuid = client1.name, client1.uuid
    client2 = RelayServerClient(relay_server.address)
    await client2.connect()
    peer2_name, peer2_uuid = client2.name, client2.uuid

    # Peer1 -> Peer2
    await client1.send(
        messages.PeerConnection(
            source_uuid=peer1_uuid,
            source_name=peer1_name,
            peer_uuid=peer2_uuid,
            description_type='offer',
            description='',
        ),
    )
    message = await asyncio.wait_for(client2.recv(), 1)
    assert isinstance(message, messages.PeerConnection)

    # Peer2 -> Peer1
    await client2.send(
        messages.PeerConnection(
            source_uuid=peer2_uuid,
            source_name=peer2_name,
            peer_uuid=peer1_uuid,
            description_type='offer',
            description='',
        ),
    )
    message = await asyncio.wait_for(client1.recv(), 1)
    assert isinstance(message, messages.PeerConnection)

    # Peer1 -> Peer1
    await client1.send(
        messages.PeerConnection(
            source_uuid=peer1_uuid,
            source_name=peer1_name,
            peer_uuid=peer1_uuid,
            description_type='offer',
            description='',
        ),
    )
    message = await asyncio.wait_for(client1.recv(), 1)
    assert isinstance(message, messages.PeerConnection)


@pytest.mark.asyncio()
async def test_p2p_message_passing_unknown_peer(relay_server) -> None:
    client1 = RelayServerClient(relay_server.address)
    await client1.connect()
    peer1_name, peer1_uuid = client1.name, client1.uuid
    peer2_uuid = uuid4()

    await client1.send(
        messages.PeerConnection(
            source_uuid=peer1_uuid,
            source_name=peer1_name,
            peer_uuid=peer2_uuid,
            description_type='offer',
            description='',
        ),
    )
    message = await asyncio.wait_for(client1.recv(), _WAIT_FOR)
    assert isinstance(message, messages.PeerConnection)
    assert message.error is not None
    assert str(peer2_uuid) in message.error
    assert 'unknown' in message.error


@pytest.mark.asyncio()
async def test_relay_server_send_encode_error(
    relay_server,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    client = RelayServerClient(relay_server.address)
    websocket = await client.connect()
    # Error should be logged but not raised
    await relay_server.relay_server.send(websocket, 'abc')

    assert any(
        [
            'Failed to encode' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_relay_server_send_connection_closed(
    relay_server,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    client = RelayServerClient(relay_server.address)
    websocket = await client.connect()
    # Error should be logged but not raised
    await websocket.close()
    await relay_server.relay_server.send(websocket, messages.Message())

    assert any(
        [
            'Connection closed' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )

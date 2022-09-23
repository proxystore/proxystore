from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

import pytest
import websockets

from proxystore.p2p import messages
from proxystore.p2p.client import connect

# Use 200ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.2


@pytest.mark.asyncio
async def test_connect_twice(signaling_server) -> None:
    uuid, _, websocket = await connect(signaling_server.address)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)
    await websocket.send(
        messages.encode(
            messages.ServerRegistration(
                name='different-host',
                uuid=uuid,
            ),
        ),
    )
    message_ = await asyncio.wait_for(websocket.recv(), _WAIT_FOR)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.ServerResponse)
    assert message.success
    assert not message.error


@pytest.mark.asyncio
async def test_connect_reconnect_new_socket(signaling_server) -> None:
    uuid1, name, websocket1 = await connect(signaling_server.address)
    pong_waiter = await websocket1.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)

    uuid2, _, websocket2 = await connect(
        signaling_server.address,
        uuid=uuid1,
        name=name,
    )
    assert uuid1 == uuid2
    await websocket1.wait_closed()
    assert websocket1.close_code != 1000
    pong_waiter = await websocket2.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio
async def test_expected_client_disconnect(signaling_server) -> None:
    uuid, _, websocket = await connect(signaling_server.address)
    client = signaling_server.signaling_server._uuid_to_client[uuid]
    assert (
        client
        in signaling_server.signaling_server._websocket_to_client.values()
    )

    await websocket.close()
    # TODO(gpauloski): remove sleep. It is here to give time for the
    # server's unregister coroutine to finish
    await asyncio.sleep(0.05)

    assert uuid not in signaling_server.signaling_server._uuid_to_client
    assert (
        client
        not in signaling_server.signaling_server._websocket_to_client.values()
    )


@pytest.mark.asyncio
async def test_unexpected_client_disconnect(signaling_server) -> None:
    uuid, _, websocket = await connect(signaling_server.address)
    client = signaling_server.signaling_server._uuid_to_client[uuid]
    assert (
        client
        in signaling_server.signaling_server._websocket_to_client.values()
    )

    await websocket.close(code=1002)
    # TODO(gpauloski): remove sleep. It is here to give time for the
    # server's unregister coroutine to finish
    await asyncio.sleep(0.05)

    assert uuid not in signaling_server.signaling_server._uuid_to_client
    assert (
        client
        not in signaling_server.signaling_server._websocket_to_client.values()
    )


@pytest.mark.asyncio
async def test_server_deserialization_fails_silently(signaling_server) -> None:
    _, _, websocket = await connect(signaling_server.address)
    # This message should cause deserialization error on server but
    # server should catch and wait for next message
    await websocket.send('invalid message')
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio
async def test_endpoint_not_registered_error(signaling_server) -> None:
    # _, _, websocket = await connect(signaling_server.address)
    websocket = await websockets.client.connect(signaling_server.address)
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


@pytest.mark.asyncio
async def test_p2p_message_passing(signaling_server) -> None:
    peer1_uuid, peer1_name, peer1 = await connect(signaling_server.address)
    peer2_uuid, peer2_name, peer2 = await connect(signaling_server.address)

    # Peer1 -> Peer2
    await peer1.send(
        messages.encode(
            messages.PeerConnection(
                source_uuid=peer1_uuid,
                source_name=peer1_name,
                peer_uuid=peer2_uuid,
                description_type='offer',
                description='',
            ),
        ),
    )
    message_ = await asyncio.wait_for(peer2.recv(), 1)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.PeerConnection)

    # Peer2 -> Peer1
    await peer2.send(
        messages.encode(
            messages.PeerConnection(
                source_uuid=peer2_uuid,
                source_name=peer2_name,
                peer_uuid=peer1_uuid,
                description_type='offer',
                description='',
            ),
        ),
    )
    message_ = await asyncio.wait_for(peer1.recv(), 1)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.PeerConnection)

    # Peer1 -> Peer1
    await peer1.send(
        messages.encode(
            messages.PeerConnection(
                source_uuid=peer1_uuid,
                source_name=peer1_name,
                peer_uuid=peer1_uuid,
                description_type='offer',
                description='',
            ),
        ),
    )
    message_ = await asyncio.wait_for(peer1.recv(), 1)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.PeerConnection)


@pytest.mark.asyncio
async def test_p2p_message_passing_unknown_peer(signaling_server) -> None:
    peer1_uuid, peer1_name, peer1 = await connect(signaling_server.address)
    peer2_uuid = uuid4()

    await peer1.send(
        messages.encode(
            messages.PeerConnection(
                source_uuid=peer1_uuid,
                source_name=peer1_name,
                peer_uuid=peer2_uuid,
                description_type='offer',
                description='',
            ),
        ),
    )
    message_ = await asyncio.wait_for(peer1.recv(), _WAIT_FOR)
    assert isinstance(message_, str)
    message = messages.decode(message_)
    assert isinstance(message, messages.PeerConnection)
    assert message.error is not None
    assert str(peer2_uuid) in message.error and 'unknown' in message.error


@pytest.mark.asyncio
async def test_signaling_server_send_encode_error(
    signaling_server,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    _, _, websocket = await connect(signaling_server.address)
    # Error should be logged but not raised
    await signaling_server.signaling_server.send(websocket, 'abc')

    assert any(
        [
            'failed to encode' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio
async def test_signaling_server_send_connection_closed(
    signaling_server,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    _, _, websocket = await connect(signaling_server.address)
    # Error should be logged but not raised
    await websocket.close()
    await signaling_server.signaling_server.send(websocket, messages.Message())

    assert any(
        [
            'connection closed' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )

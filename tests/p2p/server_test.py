from __future__ import annotations

import asyncio
import sys
from unittest import mock
from uuid import UUID
from uuid import uuid4

import pytest
import websockets

from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.messages import PeerConnectionMessage
from proxystore.p2p.messages import PeerRegistrationRequest
from proxystore.p2p.messages import PeerRegistrationResponse
from proxystore.p2p.messages import ServerError
from proxystore.p2p.server import connect
from proxystore.serialize import deserialize
from proxystore.serialize import serialize

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock

# Use 200ms as wait_for/timeout to keep test short
_WAIT_FOR = 0.2


@pytest.mark.asyncio
async def test_connect_and_ping_server(signaling_server) -> None:
    uuid, name, websocket = await connect(signaling_server.address)
    assert isinstance(uuid, UUID)
    assert isinstance(name, str)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio
async def test_connect_exceptions(signaling_server) -> None:
    async def sleep(*args, **kwargs) -> None:
        await asyncio.sleep(10)

    # Check timeout on receiving EndpointRegistationSuccess
    with mock.patch('websockets.WebSocketClientProtocol.recv', sleep):
        with pytest.raises(PeerRegistrationError, match='timeout'):
            await connect(signaling_server.address, timeout=_WAIT_FOR)

    # Check error if server returns error
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(
            return_value=serialize(
                PeerRegistrationResponse(
                    uuid=uuid4(),
                    error=Exception('abcd'),
                ),
            ),
        ),
    ):
        with pytest.raises(PeerRegistrationError, match='abcd'):
            await connect(signaling_server.address)

    # Check error if return message from signaling server is unknown type
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(return_value=serialize('nonsense message')),
    ):
        with pytest.raises(PeerRegistrationError, match='unknown'):
            await connect(signaling_server.address)

    async def close(*args, **kwargs) -> None:
        raise websockets.exceptions.ConnectionClosedError(None, None)

    # Check connection closed
    with mock.patch('websockets.WebSocketClientProtocol.recv', close):
        with pytest.raises(PeerRegistrationError, match='closed'):
            await connect(signaling_server.address)


@pytest.mark.asyncio
async def test_connect_twice(signaling_server) -> None:
    uuid, _, websocket = await connect(signaling_server.address)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)
    await websocket.send(
        serialize(PeerRegistrationRequest(uuid=uuid, name='different-host')),
    )
    message = deserialize(await asyncio.wait_for(websocket.recv(), _WAIT_FOR))
    assert isinstance(message, PeerRegistrationResponse)
    assert message.error is None
    assert message.uuid == uuid


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
    await websocket.send(b'invalid message')
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, _WAIT_FOR)


@pytest.mark.asyncio
async def test_endpoint_not_registered_error(signaling_server) -> None:
    _, _, websocket = await connect(signaling_server.address)
    websocket = await websockets.connect(f'ws://{signaling_server.address}')
    await websocket.send(
        serialize(
            PeerConnectionMessage(
                source_uuid=uuid4(),
                source_name='',
                peer_uuid=uuid4(),
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(websocket.recv(), 1))
    assert isinstance(message, ServerError)
    assert 'not registered' in str(message)


@pytest.mark.asyncio
async def test_unknown_message_type(signaling_server) -> None:
    _, _, websocket = await connect(signaling_server.address)
    await websocket.send(serialize('message'))
    message = deserialize(await asyncio.wait_for(websocket.recv(), _WAIT_FOR))
    assert isinstance(message, ServerError)
    assert 'unknown' in str(message)


@pytest.mark.asyncio
async def test_p2p_message_passing(signaling_server) -> None:
    peer1_uuid, peer1_name, peer1 = await connect(signaling_server.address)
    peer2_uuid, peer2_name, peer2 = await connect(signaling_server.address)

    # Peer1 -> Peer2
    await peer1.send(
        serialize(
            PeerConnectionMessage(
                source_uuid=peer1_uuid,
                source_name=peer1_name,
                peer_uuid=peer2_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer2.recv(), _WAIT_FOR))
    assert isinstance(message, PeerConnectionMessage)

    # Peer2 -> Peer1
    await peer2.send(
        serialize(
            PeerConnectionMessage(
                source_uuid=peer2_uuid,
                source_name=peer2_name,
                peer_uuid=peer1_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer1.recv(), _WAIT_FOR))
    assert isinstance(message, PeerConnectionMessage)

    # Peer1 -> Peer1
    await peer1.send(
        serialize(
            PeerConnectionMessage(
                source_uuid=peer1_uuid,
                source_name=peer1_name,
                peer_uuid=peer1_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer1.recv(), _WAIT_FOR))
    assert isinstance(message, PeerConnectionMessage)


@pytest.mark.asyncio
async def test_p2p_message_passing_unknown_peer(signaling_server) -> None:
    peer1_uuid, peer1_name, peer1 = await connect(signaling_server.address)
    peer2_uuid = uuid4()

    await peer1.send(
        serialize(
            PeerConnectionMessage(
                source_uuid=peer1_uuid,
                source_name=peer1_name,
                peer_uuid=peer2_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer1.recv(), _WAIT_FOR))
    assert isinstance(message, PeerConnectionMessage)
    assert str(peer2_uuid) in str(message) and 'unknown' in str(message)

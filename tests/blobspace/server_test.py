from __future__ import annotations

import asyncio
import sys
from unittest import mock

import pytest
import pytest_asyncio
import websockets

from proxystore.blobspace.exceptions import EndpointNotRegisteredError
from proxystore.blobspace.exceptions import EndpointRegistrationError
from proxystore.blobspace.exceptions import UnknownMessageType
from proxystore.blobspace.messages import EndpointRegistrationRequest
from proxystore.blobspace.messages import EndpointRegistrationSuccess
from proxystore.blobspace.messages import P2PConnectionError
from proxystore.blobspace.messages import P2PConnectionMessage
from proxystore.blobspace.server import connect
from proxystore.blobspace.server import SignalingServer
from proxystore.serialize import deserialize
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
async def servers():
    signaling_server = SignalingServer()
    async with websockets.serve(
        signaling_server.handler,
        SERVER_HOST,
        SERVER_PORT,
    ) as server:
        assert server.is_serving()
        yield server, signaling_server


@pytest.mark.asyncio
async def test_connect_and_ping_server(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    websocket = await connect('fake-uuid', 'test-client', SERVER_ADDRESS)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)


@pytest.mark.asyncio
async def test_connect_exceptions(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    async def sleep(*args, **kwargs) -> None:
        await asyncio.sleep(10)

    # Check timeout on receiving EndpointRegistationSuccess
    with mock.patch('websockets.WebSocketClientProtocol.recv', sleep):
        with pytest.raises(EndpointRegistrationError, match='timeout'):
            await connect(
                'fake-uuid',
                'test-client',
                SERVER_ADDRESS,
                timeout=0.5,
            )

    # Check error if EndpointRegistrationSuccess information does not match
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(
            return_value=serialize(
                EndpointRegistrationSuccess(
                    name='wrong-name',
                    uuid='wrong-uuid',
                ),
            ),
        ),
    ):
        with pytest.raises(EndpointRegistrationError, match='mismatched'):
            await connect('fake-uuid', 'test-client', SERVER_ADDRESS)

    # Check error if return message from signaling server is not success
    with mock.patch(
        'websockets.WebSocketClientProtocol.recv',
        AsyncMock(return_value=serialize('nonsense message')),
    ):
        with pytest.raises(EndpointRegistrationError, match='confirmation'):
            await connect('fake-uuid', 'test-client', SERVER_ADDRESS)

    async def close(*args, **kwargs) -> None:
        raise websockets.exceptions.ConnectionClosedError(None, None)

    # Check connection closed
    with mock.patch('websockets.WebSocketClientProtocol.recv', close):
        with pytest.raises(EndpointRegistrationError, match='closed'):
            await connect('fake-uuid', 'test-client', SERVER_ADDRESS)


@pytest.mark.asyncio
async def test_connect_twice(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    name = 'test-client'
    uuid = 'fake-uuid'
    websocket = await connect(uuid=uuid, name=name, address=SERVER_ADDRESS)
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)
    await websocket.send(
        serialize(EndpointRegistrationRequest(name=name, uuid=uuid)),
    )
    message = deserialize(await asyncio.wait_for(websocket.recv(), 1))
    assert isinstance(message, EndpointRegistrationSuccess)
    assert message.uuid == uuid
    assert message.name == name


@pytest.mark.asyncio
async def test_expected_client_disconnect(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    name = 'test-client'
    uuid = 'fake-uuid'
    websocket = await connect(uuid=uuid, name=name, address=SERVER_ADDRESS)
    client = servers[1]._uuid_to_client[uuid]
    assert client in servers[1]._websocket_to_client.values()

    await websocket.close()
    # TODO(gpauloski): remove sleep. It is here to give time for the
    # server's unregister coroutine to finish
    await asyncio.sleep(0.1)

    assert client.uuid not in servers[1]._uuid_to_client
    assert client not in servers[1]._websocket_to_client.values()


@pytest.mark.asyncio
async def test_unexpected_client_disconnect(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    name = 'test-client'
    uuid = 'fake-uuid'
    websocket = await connect(uuid=uuid, name=name, address=SERVER_ADDRESS)
    client = servers[1]._uuid_to_client[uuid]
    assert client in servers[1]._websocket_to_client.values()

    await websocket.close(code=1002)
    # TODO(gpauloski): remove sleep. It is here to give time for the
    # server's unregister coroutine to finish
    await asyncio.sleep(0.1)

    assert client.uuid not in servers[1]._uuid_to_client
    assert client not in servers[1]._websocket_to_client.values()


@pytest.mark.asyncio
async def test_server_deserialization_fails_silently(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    websocket = await connect(
        uuid='fake-uuid',
        name='test-client',
        address=SERVER_ADDRESS,
    )
    # This message should cause deserialization error on server but
    # server should catch and wait for next message
    await websocket.send(b'invalid message')
    pong_waiter = await websocket.ping()
    await asyncio.wait_for(pong_waiter, 1)


@pytest.mark.asyncio
async def test_endpoint_not_registered_error(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    websocket = await connect(
        uuid='fake-uuid',
        name='test-client',
        address=SERVER_ADDRESS,
    )
    websocket = await websockets.connect(f'ws://{SERVER_ADDRESS}')
    await websocket.send(serialize('message'))
    message = deserialize(await asyncio.wait_for(websocket.recv(), 1))
    assert isinstance(message, EndpointNotRegisteredError)


@pytest.mark.asyncio
async def test_unknown_message_type(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    websocket = await connect(
        uuid='fake-uuid',
        name='test-client',
        address=SERVER_ADDRESS,
    )
    await websocket.send(serialize('message'))
    message = deserialize(await asyncio.wait_for(websocket.recv(), 1))
    assert isinstance(message, UnknownMessageType)


@pytest.mark.asyncio
async def test_p2p_message_passing(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    peer1_uuid = 'fake-uuid-1'
    peer2_uuid = 'fake-uuid-2'

    peer1 = await connect(
        uuid=peer1_uuid,
        name='test-client-1',
        address=SERVER_ADDRESS,
    )
    peer2 = await connect(
        uuid=peer2_uuid,
        name='test-client-2',
        address=SERVER_ADDRESS,
    )

    # Peer1 -> Peer2
    await peer1.send(
        serialize(
            P2PConnectionMessage(
                source_uuid=peer1_uuid,
                target_uuid=peer2_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer2.recv(), 1))
    assert isinstance(message, P2PConnectionMessage)

    # Peer2 -> Peer1
    await peer2.send(
        serialize(
            P2PConnectionMessage(
                source_uuid=peer2_uuid,
                target_uuid=peer1_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer1.recv(), 1))
    assert isinstance(message, P2PConnectionMessage)

    # Peer1 -> Peer1
    await peer1.send(
        serialize(
            P2PConnectionMessage(
                source_uuid=peer1_uuid,
                target_uuid=peer1_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer1.recv(), 1))
    assert isinstance(message, P2PConnectionMessage)


@pytest.mark.asyncio
async def test_p2p_message_passing_unknown_peer(
    servers: tuple[websockets.WebSocketServer, SignalingServer],
) -> None:
    peer1_uuid = 'fake-uuid-1'
    peer2_uuid = 'fake-uuid-2'
    peer1 = await connect(
        uuid=peer1_uuid,
        name='test-client-1',
        address=SERVER_ADDRESS,
    )

    await peer1.send(
        serialize(
            P2PConnectionMessage(
                source_uuid=peer1_uuid,
                target_uuid=peer2_uuid,
                message='',
            ),
        ),
    )
    message = deserialize(await asyncio.wait_for(peer1.recv(), 1))
    assert isinstance(message, P2PConnectionError)
    assert 'unknown' in message.error

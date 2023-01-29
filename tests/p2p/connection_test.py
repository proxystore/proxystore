from __future__ import annotations

from typing import cast
from uuid import uuid4

import pytest

from proxystore.p2p import messages
from proxystore.p2p.client import connect
from proxystore.p2p.connection import MAX_CHUNK_SIZE_BYTES
from proxystore.p2p.connection import MAX_CHUNK_SIZE_STRING
from proxystore.p2p.connection import PeerConnection
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerConnectionTimeoutError


@pytest.mark.asyncio
async def test_p2p_connection(signaling_server) -> None:
    uuid1, name1, websocket1 = await connect(signaling_server.address)
    connection1 = PeerConnection(uuid1, name1, websocket1)

    uuid2, name2, websocket2 = await connect(signaling_server.address)
    connection2 = PeerConnection(uuid2, name2, websocket2)

    await connection1.send_offer(uuid2)
    offer = messages.decode(cast(str, await websocket2.recv()))
    assert isinstance(offer, messages.PeerConnection)
    await connection2.handle_server_message(offer)
    answer = messages.decode(cast(str, await websocket1.recv()))
    assert isinstance(answer, messages.PeerConnection)
    await connection1.handle_server_message(answer)

    await connection1.ready()
    await connection2.ready()

    assert connection1.state == 'connected'
    assert connection2.state == 'connected'

    # Very long string message to test chunking
    message_str = 'x' * MAX_CHUNK_SIZE_STRING * 3
    await connection1.send(message_str)
    assert await connection2.recv() == message_str
    await connection2.send('hello hello')
    assert await connection1.recv() == 'hello hello'

    # Very long bytes message to test chunking
    message_bytes = b'\x00' * MAX_CHUNK_SIZE_BYTES * 3
    await connection1.send(message_bytes)
    assert await connection2.recv() == message_bytes
    await connection2.send(b'hello hello')
    assert await connection1.recv() == b'hello hello'

    await websocket1.close()
    await websocket2.close()
    await connection1.close()
    await connection2.close()


@pytest.mark.asyncio
async def test_p2p_connection_multichannel(signaling_server) -> None:
    uuid1, name1, websocket1 = await connect(signaling_server.address)
    # Set channels as different to verify the answerer respects the
    # number of channels from the offerer
    connection1 = PeerConnection(uuid1, name1, websocket1, channels=4)

    uuid2, name2, websocket2 = await connect(signaling_server.address)
    connection2 = PeerConnection(uuid2, name2, websocket2, channels=1)

    await connection1.send_offer(uuid2)
    offer = messages.decode(cast(str, await websocket2.recv()))
    assert isinstance(offer, messages.PeerConnection)
    await connection2.handle_server_message(offer)
    answer = messages.decode(cast(str, await websocket1.recv()))
    assert isinstance(answer, messages.PeerConnection)
    await connection1.handle_server_message(answer)

    await connection1.ready()
    await connection2.ready()

    assert len(connection1._channels) == 4
    assert len(connection2._channels) == 4

    await websocket1.close()
    await websocket2.close()
    await connection1.close()
    await connection2.close()


@pytest.mark.asyncio
async def test_p2p_connection_timeout(signaling_server) -> None:
    uuid1, name1, websocket1 = await connect(signaling_server.address)
    connection1 = PeerConnection(uuid1, name1, websocket1)

    uuid2, name2, websocket2 = await connect(signaling_server.address)
    connection2 = PeerConnection(uuid2, name2, websocket2)

    await connection1.send_offer(uuid2)
    # Don't finish offer/answer sending so wait() times out

    with pytest.raises(PeerConnectionTimeoutError):
        await connection1.ready(timeout=0.05)

    await websocket1.close()
    await websocket2.close()
    await connection1.close()
    await connection2.close()


@pytest.mark.asyncio
async def test_p2p_connection_error(signaling_server) -> None:
    uuid, name, websocket = await connect(signaling_server.address)
    connection = PeerConnection(uuid, name, websocket)

    class MyError(Exception):
        pass

    await connection.handle_server_message(
        messages.PeerConnection(
            source_uuid=uuid,
            source_name=name,
            peer_uuid=uuid4(),
            description_type='offer',
            description='',
            error=str(MyError()),
        ),
    )

    with pytest.raises(PeerConnectionError):
        await connection.ready()

    await websocket.close()

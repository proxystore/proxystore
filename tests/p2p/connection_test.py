from __future__ import annotations

import asyncio
from uuid import uuid4

import aiortc
import pytest

from proxystore.p2p.connection import MAX_CHUNK_SIZE_BYTES
from proxystore.p2p.connection import MAX_CHUNK_SIZE_STRING
from proxystore.p2p.connection import PeerConnection
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerConnectionTimeoutError
from proxystore.p2p.relay.client import RelayClient
from proxystore.p2p.relay.messages import PeerConnectionRequest


@pytest.mark.asyncio()
async def test_p2p_connection(relay_server) -> None:
    client1 = RelayClient(relay_server.address)
    await client1.connect()
    connection1 = PeerConnection(client1)

    client2 = RelayClient(relay_server.address)
    await client2.connect()
    connection2 = PeerConnection(client2)

    await connection1.send_offer(client2.uuid)
    offer = await client2.recv()
    assert isinstance(offer, PeerConnectionRequest)
    await connection2.handle_server_message(offer)
    answer = await client1.recv()
    assert isinstance(answer, PeerConnectionRequest)
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

    await client1.close()
    await client2.close()
    await connection1.close()
    await connection2.close()


@pytest.mark.asyncio()
async def test_p2p_connection_multichannel(relay_server) -> None:
    client1 = RelayClient(relay_server.address)
    await client1.connect()
    # Set channels as different to verify the answerer respects the
    # number of channels from the offerer
    connection1 = PeerConnection(client1, channels=4)

    client2 = RelayClient(relay_server.address)
    await client2.connect()
    connection2 = PeerConnection(client2, channels=1)

    await connection1.send_offer(client2.uuid)
    offer = await client2.recv()
    assert isinstance(offer, PeerConnectionRequest)
    await connection2.handle_server_message(offer)
    answer = await client1.recv()
    assert isinstance(answer, PeerConnectionRequest)
    await connection1.handle_server_message(answer)

    await connection1.ready()
    await connection2.ready()

    assert len(connection1._channels) == 4
    assert len(connection2._channels) == 4

    await client1.close()
    await client2.close()
    await connection1.close()
    await connection2.close()


@pytest.mark.asyncio()
async def test_p2p_connection_timeout(relay_server) -> None:
    client1 = RelayClient(relay_server.address)
    await client1.connect()
    connection1 = PeerConnection(client1)

    client2 = RelayClient(relay_server.address)
    await client2.connect()
    connection2 = PeerConnection(client2)

    await connection1.send_offer(client2.uuid)
    # Don't finish offer/answer sending so wait() times out

    with pytest.raises(PeerConnectionTimeoutError):
        await connection1.ready(timeout=0.05)

    await client1.close()
    await client2.close()
    await connection1.close()
    await connection2.close()


@pytest.mark.asyncio()
async def test_p2p_connection_error(relay_server) -> None:
    client = RelayClient(relay_server.address)
    await client.connect()
    connection = PeerConnection(client)

    class MyError(Exception):
        pass

    await connection.handle_server_message(
        PeerConnectionRequest(
            source_uuid=client.uuid,
            source_name=client.name,
            peer_uuid=uuid4(),
            description_type='offer',
            description='',
            error=str(MyError()),
        ),
    )

    with pytest.raises(PeerConnectionError):
        await connection.ready()

    await client.close()


@pytest.mark.asyncio()
async def test_p2p_closed_by_offerer_callback(relay_server) -> None:
    closed_event_1 = asyncio.Event()
    closed_event_2 = asyncio.Event()

    async def closed_callback_1() -> None:
        closed_event_1.set()

    async def closed_callback_2(a: int, *, b: int) -> None:
        assert a == 1
        assert b == 2
        closed_event_2.set()

    client1 = RelayClient(relay_server.address)
    await client1.connect()
    connection1 = PeerConnection(client1)
    connection1.on_close_callback(closed_callback_1)

    client2 = RelayClient(relay_server.address)
    await client2.connect()
    connection2 = PeerConnection(client2)
    connection2.on_close_callback(closed_callback_2, 1, b=2)

    await connection1.send_offer(client2.uuid)
    offer = await client2.recv()
    assert isinstance(offer, PeerConnectionRequest)
    await connection2.handle_server_message(offer)
    answer = await client1.recv()
    assert isinstance(answer, PeerConnectionRequest)
    await connection1.handle_server_message(answer)

    await connection1.ready()
    await connection2.ready()

    assert connection1.state == 'connected'
    assert connection2.state == 'connected'

    assert not closed_event_1.is_set()
    assert not closed_event_2.is_set()

    await connection1.close()

    assert connection1.state == 'closed'
    assert closed_event_1.is_set()

    with pytest.raises(aiortc.exceptions.InvalidStateError):
        await connection2.send(b'message')

    assert connection2.state == 'closed'
    assert closed_event_2.is_set()

    await client1.close()
    await client2.close()


@pytest.mark.asyncio()
async def test_p2p_closed_by_answerer_callback(relay_server) -> None:
    closed_event_1 = asyncio.Event()
    closed_event_2 = asyncio.Event()

    async def closed_callback_1() -> None:
        closed_event_1.set()

    async def closed_callback_2() -> None:
        closed_event_2.set()

    client1 = RelayClient(relay_server.address)
    await client1.connect()
    connection1 = PeerConnection(client1)
    connection1.on_close_callback(closed_callback_1)

    client2 = RelayClient(relay_server.address)
    await client2.connect()
    connection2 = PeerConnection(client2)
    connection2.on_close_callback(closed_callback_2)

    await connection1.send_offer(client2.uuid)
    offer = await client2.recv()
    assert isinstance(offer, PeerConnectionRequest)
    await connection2.handle_server_message(offer)
    answer = await client1.recv()
    assert isinstance(answer, PeerConnectionRequest)
    await connection1.handle_server_message(answer)

    await connection1.ready()
    await connection2.ready()

    assert connection1.state == 'connected'
    assert connection2.state == 'connected'

    assert not closed_event_1.is_set()
    assert not closed_event_2.is_set()

    await connection2.close()

    assert connection2.state == 'closed'
    assert closed_event_2.is_set()

    with pytest.raises(aiortc.exceptions.InvalidStateError):
        await connection1.send(b'message')

    assert connection1.state == 'closed'
    assert closed_event_1.is_set()

    await client1.close()
    await client2.close()

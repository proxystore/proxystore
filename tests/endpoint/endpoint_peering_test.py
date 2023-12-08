from __future__ import annotations

import logging
import uuid
from typing import AsyncGenerator

import pytest
import pytest_asyncio

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.messages import EndpointRequest
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.relay.client import RelayClient
from proxystore.serialize import serialize
from testing.compat import randbytes


@pytest_asyncio.fixture()
async def endpoints(
    relay_server,
) -> AsyncGenerator[tuple[Endpoint, Endpoint], None]:
    relay_client_1 = RelayClient(
        relay_server.address,
        client_name='test-endpoint-1',
    )
    relay_client_2 = RelayClient(
        relay_server.address,
        client_name='test-endpoint-2',
    )
    peer_manager_1 = await PeerManager(relay_client_1)
    peer_manager_2 = await PeerManager(relay_client_2)
    async with Endpoint(peer_manager=peer_manager_1) as ep1:
        async with Endpoint(peer_manager=peer_manager_2) as ep2:
            yield (ep1, ep2)


@pytest.mark.asyncio()
async def test_init(relay_server) -> None:
    relay_client = RelayClient(
        relay_server.address,
        client_name='test-init-endpoint',
    )
    peer_manager = await PeerManager(relay_client)
    endpoint = await Endpoint(peer_manager=peer_manager)
    # Calling async_init multiple times should be no-op
    await endpoint.async_init()
    await endpoint.async_init()
    await endpoint.close()


@pytest.mark.asyncio()
async def test_set(endpoints: tuple[Endpoint, Endpoint]) -> None:
    endpoint1, endpoint2 = endpoints
    key = str(uuid.uuid4())
    data = randbytes(100)
    await endpoint1.set(key, data, endpoint=endpoint2.uuid)
    assert (await endpoint2.get(key)) == data


@pytest.mark.asyncio()
async def test_get(endpoints: tuple[Endpoint, Endpoint]) -> None:
    endpoint1, endpoint2 = endpoints
    data1 = randbytes(100)
    key = str(uuid.uuid4())
    await endpoint1.set(key, data1, endpoint=endpoint2.uuid)
    assert (await endpoint1.get(key, endpoint=endpoint2.uuid)) == data1
    assert (await endpoint2.get(key)) == data1

    data2 = randbytes(100)
    await endpoint2.set(key, data2)
    assert (await endpoint1.get(key)) is None
    assert (await endpoint2.get(key)) == data2

    assert (await endpoint2.get('missingkey', endpoint=endpoint1.uuid)) is None


@pytest.mark.asyncio()
async def test_evict(endpoints: tuple[Endpoint, Endpoint]) -> None:
    endpoint1, endpoint2 = endpoints
    data = randbytes(100)
    key = str(uuid.uuid4())
    await endpoint1.set(key, data)
    # Should not do anything because key is not on endpoint2
    await endpoint2.evict(key, endpoint=endpoint2.uuid)
    assert (await endpoint1.get(key)) == data
    # Evict on remote endpoint
    await endpoint2.evict(key, endpoint=endpoint1.uuid)
    assert (await endpoint1.get(key)) is None


@pytest.mark.asyncio()
async def test_exists(endpoints: tuple[Endpoint, Endpoint]) -> None:
    endpoint1, endpoint2 = endpoints
    data = randbytes(100)
    key = str(uuid.uuid4())
    assert not (await endpoint2.exists(key))
    await endpoint1.set(key, data, endpoint=endpoint2.uuid)
    assert await endpoint2.exists(key)


@pytest.mark.asyncio()
async def test_remote_error_propogation(
    endpoints: tuple[Endpoint, Endpoint],
) -> None:
    endpoint1, endpoint2 = endpoints
    key = str(uuid.uuid4())
    with pytest.raises(AssertionError):
        await endpoint1.set(key, None, endpoint=endpoint2.uuid)  # type: ignore


@pytest.mark.asyncio()
async def test_peering_not_available(relay_server) -> None:
    relay_client = RelayClient(
        relay_server.address,
        client_name='test-peering-not-available',
    )
    peer_manager = await PeerManager(relay_client)
    endpoint = Endpoint(peer_manager=peer_manager)
    # __await__ has not been called on endpoint so connection to server
    # has not been enabled
    with pytest.raises(PeeringNotAvailableError, match='await'):
        await endpoint.get('key', endpoint=uuid.uuid4())
    await peer_manager.close()


@pytest.mark.asyncio()
async def test_delayed_peer_manager_async_init(relay_server) -> None:
    relay_client = RelayClient(
        relay_server.address,
        client_name='test-unknown-peer',
    )
    # The peer manager is not initialized with await so using it would raise
    # an error but the async initialization of the Endpoint will call
    # the peer manager's async_init()
    peer_manager = PeerManager(relay_client)
    with pytest.raises(RuntimeError, match='await'):
        assert peer_manager.relay_client is not None
    async with Endpoint(peer_manager=peer_manager):
        assert peer_manager.relay_client is not None


@pytest.mark.asyncio()
async def test_unknown_peer(relay_server) -> None:
    relay_client = RelayClient(
        relay_server.address,
        client_name='test-unknown-peer',
    )
    peer_manager = await PeerManager(relay_client)
    async with Endpoint(peer_manager=peer_manager) as endpoint:
        with pytest.raises(
            PeerRequestError,
            match='Cannot forward peer connection message to peer',
        ):
            await endpoint.get('key', endpoint=uuid.uuid4())


@pytest.mark.asyncio()
async def test_unsupported_peer_message(
    endpoints: tuple[Endpoint, Endpoint],
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    endpoint1, endpoint2 = endpoints

    assert endpoint2._peer_manager is not None
    endpoint2._peer_manager._message_queue.put_nowait(
        (endpoint1.uuid, b'nonsense_message'),
    )
    # Make request to endpoint 2 to establish connection
    key = str(uuid.uuid4())
    assert not (await endpoint1.exists(key, endpoint=endpoint2.uuid))

    assert any(
        [
            'unable to decode message from peer' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_unexpected_response(
    endpoints: tuple[Endpoint, Endpoint],
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    endpoint1, endpoint2 = endpoints

    # Make request to open peer connection
    assert not (await endpoint1.exists('key', endpoint=endpoint2.uuid))

    # Add bad message to queue
    message = serialize(
        EndpointRequest(
            kind='request',
            op='evict',
            uuid='1234',
            key='key',
        ),
    )
    assert endpoint2._peer_manager is not None
    endpoint2._peer_manager._message_queue.put_nowait(
        (endpoint1.uuid, message),
    )

    # Make request to endpoint 2 to flush queue
    assert not (await endpoint1.exists('key', endpoint=endpoint2.uuid))

    assert any(
        [
            'does not match' in record.message and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )

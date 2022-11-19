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
from proxystore.serialize import serialize
from testing.compat import randbytes


@pytest_asyncio.fixture(scope='module')
async def endpoints(
    signaling_server,
) -> AsyncGenerator[tuple[Endpoint, Endpoint], None]:
    async with Endpoint(
        name='test-endpoint-1',
        uuid=uuid.uuid4(),
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name='test-endpoint-2',
        uuid=uuid.uuid4(),
        signaling_server=signaling_server.address,
    ) as endpoint2:
        yield (endpoint1, endpoint2)


@pytest.mark.asyncio
async def test_init(signaling_server) -> None:
    endpoint = await Endpoint(
        name='test-init-endpoint',
        uuid=uuid.uuid4(),
        signaling_server=signaling_server.address,
    )
    # Calling async_init multiple times should be no-op
    await endpoint.async_init()
    await endpoint.async_init()
    await endpoint.close()


@pytest.mark.asyncio
async def test_set(endpoints: tuple[Endpoint, Endpoint]) -> None:
    endpoint1, endpoint2 = endpoints
    key = str(uuid.uuid4())
    data = randbytes(100)
    await endpoint1.set(key, data, endpoint=endpoint2.uuid)
    assert (await endpoint2.get(key)) == data


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_exists(endpoints: tuple[Endpoint, Endpoint]) -> None:
    endpoint1, endpoint2 = endpoints
    data = randbytes(100)
    key = str(uuid.uuid4())
    assert not (await endpoint2.exists(key))
    await endpoint1.set(key, data, endpoint=endpoint2.uuid)
    assert await endpoint2.exists(key)


@pytest.mark.asyncio
async def test_remote_error_propogation(
    endpoints: tuple[Endpoint, Endpoint],
) -> None:
    endpoint1, endpoint2 = endpoints
    key = str(uuid.uuid4())
    with pytest.raises(AssertionError):
        ep = endpoint2.uuid
        await endpoint1.set(key, None, endpoint=ep)  # type: ignore


@pytest.mark.asyncio
async def test_peering_not_available(signaling_server) -> None:
    endpoint = Endpoint(
        name='test',
        uuid=uuid.uuid4(),
        signaling_server=signaling_server.address,
    )
    # __await__ has not been called on endpoint so connection to server
    # has not been enabled
    with pytest.raises(PeeringNotAvailableError, match='await'):
        await endpoint.get('key', endpoint=uuid.uuid4())


@pytest.mark.asyncio
async def test_unknown_peer(signaling_server) -> None:
    async with Endpoint(
        name='test',
        uuid=uuid.uuid4(),
        signaling_server=signaling_server.address,
    ) as endpoint:
        with pytest.raises(PeerRequestError, match='unknown'):
            await endpoint.get('key', endpoint=uuid.uuid4())


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_unexpected_response(
    endpoints: tuple[Endpoint, Endpoint],
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    endpoint1, endpoint2 = endpoints

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

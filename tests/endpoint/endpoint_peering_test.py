from __future__ import annotations

import dataclasses
import json
import logging
import uuid

import pytest

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.messages import EndpointRequest
from testing.compat import randbytes

_NAME1 = 'test-endpoint-1'
_NAME2 = 'test-endpoint-2'
_UUID1 = uuid.uuid4()
_UUID2 = uuid.uuid4()


@pytest.mark.asyncio
async def test_init(signaling_server) -> None:
    endpoint = await Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    )
    # Calling async_init multiple times should be no-op
    await endpoint.async_init()
    await endpoint.async_init()
    await endpoint.close()


@pytest.mark.asyncio
async def test_set(signaling_server) -> None:
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name=_NAME2,
        uuid=_UUID2,
        signaling_server=signaling_server.address,
    ) as endpoint2:
        data = randbytes(100)
        await endpoint1.set('key', data, endpoint=endpoint2.uuid)
        assert (await endpoint2.get('key')) == data


@pytest.mark.asyncio
async def test_get(signaling_server) -> None:
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name=_NAME2,
        uuid=_UUID2,
        signaling_server=signaling_server.address,
    ) as endpoint2:
        data1 = randbytes(100)
        await endpoint1.set('key', data1, endpoint=endpoint2.uuid)
        assert (await endpoint1.get('key', endpoint=endpoint2.uuid)) == data1
        assert (await endpoint2.get('key')) == data1

        data2 = randbytes(100)
        await endpoint2.set('key', data2)
        assert (await endpoint1.get('key')) is None
        assert (await endpoint2.get('key')) == data2

        assert (
            await endpoint2.get('missingkey', endpoint=endpoint1.uuid)
        ) is None


@pytest.mark.asyncio
async def test_evict(signaling_server) -> None:
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name=_NAME2,
        uuid=_UUID2,
        signaling_server=signaling_server.address,
    ) as endpoint2:
        data = randbytes(100)
        await endpoint1.set('key', data)
        # Should not do anything because key is not on endpoint2
        await endpoint2.evict('key', endpoint=endpoint2.uuid)
        assert (await endpoint1.get('key')) == data
        # Evict on remote endpoint
        await endpoint2.evict('key', endpoint=endpoint1.uuid)
        assert (await endpoint1.get('key')) is None


@pytest.mark.asyncio
async def test_exists(signaling_server) -> None:
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name=_NAME2,
        uuid=_UUID2,
        signaling_server=signaling_server.address,
    ) as endpoint2:
        data = randbytes(100)
        assert not (await endpoint2.exists('key'))
        await endpoint1.set('key', data, endpoint=endpoint2.uuid)
        assert await endpoint2.exists('key')


@pytest.mark.asyncio
async def test_peering_not_available(signaling_server) -> None:
    endpoint = Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    )
    # __await__ has not been called on endpoint so connection to server
    # has not been enabled
    with pytest.raises(PeeringNotAvailableError, match='await'):
        await endpoint.get('key', endpoint=uuid.uuid4())


@pytest.mark.asyncio
async def test_unknown_peer(signaling_server) -> None:
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint:
        with pytest.raises(PeerRequestError, match='unknown'):
            await endpoint.get('key', endpoint=uuid.uuid4())


@pytest.mark.asyncio
async def test_unsupported_peer_message(signaling_server, caplog) -> None:
    caplog.set_level(logging.ERROR)
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name=_NAME2,
        uuid=_UUID2,
        signaling_server=signaling_server.address,
    ) as endpoint2:
        assert endpoint2._peer_manager is not None
        endpoint2._peer_manager._message_queue.put_nowait(
            (endpoint1.uuid, 'nonsense_message'),
        )
        # Make request to endpoint 2 to establish connection
        assert not (await endpoint1.exists('key', endpoint=endpoint2.uuid))

    assert any(
        [
            'unable to decode message from peer' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio
async def test_unexpected_response(signaling_server, caplog) -> None:
    caplog.set_level(logging.ERROR)
    async with Endpoint(
        name=_NAME1,
        uuid=_UUID1,
        signaling_server=signaling_server.address,
    ) as endpoint1, Endpoint(
        name=_NAME2,
        uuid=_UUID2,
        signaling_server=signaling_server.address,
    ) as endpoint2:
        # Force connection to establish
        assert endpoint1._peer_manager is not None
        assert endpoint2._peer_manager is not None
        connection = await endpoint1._peer_manager.get_connection(
            endpoint2.uuid,
        )
        await connection.ready()

        # Add bad message to queue
        message = json.dumps(
            dataclasses.asdict(
                EndpointRequest(
                    kind='request',
                    op='evict',
                    uuid='1234',
                    key='key',
                ),
            ),
        )
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

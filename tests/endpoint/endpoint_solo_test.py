from __future__ import annotations

import uuid

import pytest

from proxystore.endpoint.endpoint import Endpoint
from testing.compat import randbytes

_NAME = 'test-endpoint'
_UUID = uuid.uuid4()


@pytest.mark.asyncio()
async def test_init() -> None:
    endpoint = Endpoint(name=_NAME, uuid=_UUID)
    # Should not do anything
    await endpoint.close()

    # Try again with awaitable initialization
    endpoint = await Endpoint(name=_NAME, uuid=_UUID)
    await endpoint.close()


@pytest.mark.asyncio()
async def test_init_requires_name_uuid() -> None:
    message = (
        'The name and uuid parameters must be provided if a PeerManager '
        'is not provided.'
    )
    with pytest.raises(ValueError, match=message):
        Endpoint(name='test')
    with pytest.raises(ValueError, match=message):
        Endpoint(uuid=uuid.uuid4())


@pytest.mark.asyncio()
async def test_set() -> None:
    async with Endpoint(name=_NAME, uuid=_UUID) as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data

        # Check key gets overwritten
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data


@pytest.mark.asyncio()
async def test_get() -> None:
    async with Endpoint(name=_NAME, uuid=_UUID) as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data
        assert (await endpoint.get('key', endpoint=uuid.uuid4())) == data


@pytest.mark.asyncio()
async def test_evict() -> None:
    async with Endpoint(name=_NAME, uuid=_UUID) as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data
        await endpoint.evict('key')
        assert (await endpoint.get('key')) is None
        # Should not raise error if key does not exists already
        await endpoint.evict('key')


@pytest.mark.asyncio()
async def test_exists() -> None:
    async with Endpoint(name=_NAME, uuid=_UUID) as endpoint:
        data = randbytes(100)
        assert not (await endpoint.exists('key'))
        await endpoint.set('key', data)
        assert await endpoint.exists('key')

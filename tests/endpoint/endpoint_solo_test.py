from __future__ import annotations

import pytest

from proxystore.endpoint.endpoint import Endpoint
from testing.compat import randbytes


@pytest.mark.asyncio
async def test_init() -> None:
    endpoint = Endpoint()
    # Should not do anything
    await endpoint.close()

    # Try again with awaitable initialization
    endpoint = await Endpoint()
    await endpoint.close()


@pytest.mark.asyncio
async def test_set() -> None:
    async with Endpoint() as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data

        # Check key gets overwritten
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data


@pytest.mark.asyncio
async def test_get() -> None:
    async with Endpoint() as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data
        assert (await endpoint.get('key', endpoint='random-endpoint')) == data


@pytest.mark.asyncio
async def test_evict() -> None:
    async with Endpoint() as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data
        await endpoint.evict('key')
        assert (await endpoint.get('key')) is None
        # Should not raise error if key does not exists already
        await endpoint.evict('key')


@pytest.mark.asyncio
async def test_exists() -> None:
    async with Endpoint() as endpoint:
        data = randbytes(100)
        assert not (await endpoint.exists('key'))
        await endpoint.set('key', data)
        assert await endpoint.exists('key')

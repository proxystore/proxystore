from __future__ import annotations

import pytest

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import PeeringNotAvailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import PingResult
from testing.compat import randbytes

_NAME = 'test-endpoint'
_ID = EndpointId.random()


@pytest.mark.asyncio
async def test_init() -> None:
    endpoint = Endpoint(name=_NAME, endpoint_id=_ID)
    # Should not do anything
    await endpoint.close()

    # Try again with awaitable initialization
    endpoint = await Endpoint(name=_NAME, endpoint_id=_ID)
    await endpoint.close()
    # Closing again is a no-op
    await endpoint.close()


@pytest.mark.asyncio
async def test_set() -> None:
    async with Endpoint(name=_NAME, endpoint_id=_ID) as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data

        # Check key gets overwritten
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data


@pytest.mark.asyncio
async def test_get() -> None:
    async with Endpoint(name=_NAME, endpoint_id=_ID) as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data
        assert (await endpoint.get('key', endpoint=_ID)) == data


@pytest.mark.parametrize('op', ('evict', 'exists', 'get', 'set'))
@pytest.mark.asyncio
async def test_remote_endpoint_not_available(op: str) -> None:
    async with Endpoint(name=_NAME, endpoint_id=_ID) as endpoint:
        args = ('key', b'data') if op == 'set' else ('key',)
        with pytest.raises(PeeringNotAvailableError):
            await getattr(endpoint, op)(*args, endpoint=EndpointId.random())


@pytest.mark.asyncio
async def test_evict() -> None:
    async with Endpoint(name=_NAME, endpoint_id=_ID) as endpoint:
        data = randbytes(100)
        await endpoint.set('key', data)
        assert (await endpoint.get('key')) == data
        await endpoint.evict('key')
        assert (await endpoint.get('key')) is None
        # Should not raise error if key does not exists already
        await endpoint.evict('key')


@pytest.mark.asyncio
async def test_exists() -> None:
    async with Endpoint(name=_NAME, endpoint_id=_ID) as endpoint:
        data = randbytes(100)
        assert not (await endpoint.exists('key'))
        await endpoint.set('key', data)
        assert await endpoint.exists('key')


@pytest.mark.asyncio
async def test_ping() -> None:
    async with Endpoint(name=_NAME, endpoint_id=_ID) as endpoint:
        assert await endpoint.ping() == PingResult()
        with pytest.raises(PeeringNotAvailableError):
            await endpoint.ping(EndpointId.random())

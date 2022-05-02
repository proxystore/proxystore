from __future__ import annotations

from typing import AsyncGenerator

import pytest
import pytest_asyncio
import quart

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.serve import create_app
from testing.compat import randbytes


@pytest_asyncio.fixture
@pytest.mark.asyncio
async def quart_app() -> AsyncGenerator[quart.Quart, None]:
    async with Endpoint() as endpoint:
        app = create_app(endpoint)
        async with app.test_app() as test_app:
            yield test_app


@pytest.mark.asyncio
async def test_running(quart_app) -> None:
    client = quart_app.test_client()
    response = await client.get('/')
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_set_request(quart_app) -> None:
    client = quart_app.test_client()
    data = randbytes(100)
    set_response = await client.post(
        '/set',
        headers={'Content-Type': 'application/octet-stream'},
        query_string={'key': 'my-key'},
        data=data,
    )
    assert set_response.status_code == 200

    # overwrite key should be okay
    data = randbytes(100)
    set_response = await client.post(
        '/set',
        headers={'Content-Type': 'application/octet-stream'},
        query_string={'key': 'my-key'},
        data=data,
    )
    assert set_response.status_code == 200


@pytest.mark.asyncio
async def test_get_request(quart_app) -> None:
    client = quart_app.test_client()
    data = randbytes(100)
    set_response = await client.post(
        '/set',
        headers={'Content-Type': 'application/octet-stream'},
        query_string={'key': 'my-key'},
        data=data,
    )
    assert set_response.status_code == 200

    get_response = await client.get('/get', query_string={'key': 'my-key'})
    assert get_response.status_code == 200
    assert (await get_response.get_data()) == data


@pytest.mark.asyncio
async def test_exists_request(quart_app) -> None:
    client = quart_app.test_client()
    exists_response = await client.get(
        'exists',
        query_string={'key': 'my-key'},
    )
    assert exists_response.status_code == 200
    assert not (await exists_response.get_json())['exists']

    data = randbytes(100)
    set_response = await client.post(
        '/set',
        headers={'Content-Type': 'application/octet-stream'},
        query_string={'key': 'my-key'},
        data=data,
    )
    assert set_response.status_code == 200

    exists_response = await client.get(
        'exists',
        query_string={'key': 'my-key'},
    )
    assert exists_response.status_code == 200
    assert (await exists_response.get_json())['exists']


@pytest.mark.asyncio
async def test_evict_request(quart_app) -> None:
    client = quart_app.test_client()
    evict_response = await client.post('evict', query_string={'key': 'my-key'})
    # No error if key does not exist
    assert evict_response.status_code == 200

    data = randbytes(100)
    set_response = await client.post(
        '/set',
        headers={'Content-Type': 'application/octet-stream'},
        query_string={'key': 'my-key'},
        data=data,
    )
    assert set_response.status_code == 200

    exists_response = await client.get(
        'exists',
        query_string={'key': 'my-key'},
    )
    assert exists_response.status_code == 200
    assert (await exists_response.get_json())['exists']

    evict_response = await client.post('evict', query_string={'key': 'my-key'})
    assert evict_response.status_code == 200

    exists_response = await client.get(
        'exists',
        query_string={'key': 'my-key'},
    )
    assert exists_response.status_code == 200
    assert not (await exists_response.get_json())['exists']

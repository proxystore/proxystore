from __future__ import annotations

import contextlib
import logging
import multiprocessing
import os
import time
import uuid
from typing import AsyncGenerator
from unittest import mock

import pytest
import pytest_asyncio
import quart
import requests

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.serve import create_app
from proxystore.endpoint.serve import serve
from testing.compat import randbytes


@pytest_asyncio.fixture
@pytest.mark.asyncio
async def quart_app() -> AsyncGenerator[quart.Quart, None]:
    async with Endpoint(
        name='my-endpoint',
        uuid=str(uuid.uuid4()),
    ) as endpoint:
        app = create_app(endpoint)
        async with app.test_app() as test_app:
            yield test_app


@pytest.mark.asyncio
async def test_running(quart_app) -> None:
    client = quart_app.test_client()
    response = await client.get('/')
    assert response.status_code == 200

    response = await client.get('/endpoint')
    assert len((await response.get_json())['uuid']) > 0


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

    get_response = await client.get(
        '/get',
        query_string={'key': 'missing-key'},
    )
    assert get_response.status_code == 400


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


@pytest.mark.timeout(5)
def test_serve() -> None:
    name = 'my-endpoint'
    uuid_ = str(uuid.uuid4())
    host = 'localhost'
    port = 5823

    def serve_without_stdout() -> None:
        with contextlib.redirect_stdout(None), contextlib.redirect_stderr(
            None,
        ):
            logging.disable(10000)
            serve(name=name, uuid=uuid_, host=host, port=port)

    process = multiprocessing.Process(target=serve_without_stdout)
    process.start()

    try:
        while True:
            try:
                r = requests.get(f'http://{host}:{port}/')
            except requests.exceptions.ConnectionError:
                time.sleep(0.01)
                continue
            if r.status_code == 200:  # pragma: no branch
                break
    finally:
        process.terminate()


@mock.patch('quart.Quart.run')
def test_serve_logging(mock_run, tmp_dir) -> None:
    with contextlib.redirect_stdout(None), contextlib.redirect_stderr(
        None,
    ):
        # Make directory if necessary
        log_file = os.path.join(tmp_dir, 'log.txt')
        serve('name', 'uuid', '0.0.0.0', 1234, None, 'INFO', log_file)
        print(os.listdir(tmp_dir))
        assert os.path.isdir(tmp_dir)
        assert os.path.exists(log_file)

        # Write log to existing log directory
        log_file2 = os.path.join(tmp_dir, 'log2.txt')
        serve('name', 'uuid', '0.0.0.0', 1234, None, 'INFO', log_file2)
        assert os.path.isdir(tmp_dir)
        assert os.path.exists(log_file2)

"""EndpointStore Unit Tests."""
from __future__ import annotations

import os
import shutil
import uuid
from unittest import mock

import pytest
import requests

from proxystore.endpoint.config import update_config
from proxystore.store.endpoint import EndpointStore
from proxystore.store.endpoint import EndpointStoreError


def test_default_config(tmp_dir) -> None:
    response = requests.Response()
    response.json = lambda: {'uuid': str(uuid.uuid4())}
    response.status_code = 200

    update_config(tmp_dir, host='localhost', port=1234)
    with mock.patch(
        'proxystore.endpoint.config.default_dir',
        return_value=tmp_dir,
    ):
        with mock.patch('requests.get', return_value=response):
            store = EndpointStore('name')

    assert store.hostname == 'localhost'
    assert store.port == 1234


def test_load_from_config(tmp_dir) -> None:
    update_config(tmp_dir, host='localhost', port=1234)

    response = requests.Response()
    response.json = lambda: {'uuid': str(uuid.uuid4())}
    response.status_code = 200

    with mock.patch('requests.get', return_value=response):
        store = EndpointStore('name', endpoint_dir=tmp_dir)

    assert os.path.exists(tmp_dir)
    assert store.hostname == 'localhost'
    assert store.port == 1234

    shutil.rmtree(tmp_dir)


def test_bad_config(tmp_dir) -> None:
    update_config(tmp_dir, host=None)
    with pytest.raises(ValueError, match='host and port'):
        EndpointStore('name', endpoint_dir=tmp_dir)

    update_config(tmp_dir, host='localhost', port=None)
    with pytest.raises(ValueError, match='host and port'):
        EndpointStore('name', endpoint_dir=tmp_dir)


def test_override_config(tmp_dir) -> None:
    with pytest.raises(ValueError, match='hostname and port'):
        EndpointStore('name', hostname='localhost', port=None)

    with pytest.raises(ValueError, match='hostname and port'):
        EndpointStore('name', hostname=None, port=1234)

    tmp_dir = f'/tmp/{uuid.uuid4()}'
    update_config(tmp_dir, host='fake-localhost', port=4321)

    response = requests.Response()
    response.json = lambda: {'uuid': str(uuid.uuid4())}
    response.status_code = 200

    with mock.patch('requests.get', return_value=response):
        store = EndpointStore(
            'name',
            hostname='localhost',
            port=1234,
            endpoint_dir=tmp_dir,
        )
    assert store.hostname == 'localhost'
    assert store.port == 1234


def test_bad_responses(endpoint_store) -> None:
    """Test handling of bad responses from Endpoint."""
    store = EndpointStore(
        endpoint_store.name,
        **endpoint_store.kwargs,
        cache_size=0,
    )

    response = requests.Response()
    response.status_code = 400

    with mock.patch('requests.get', return_value=response):
        with pytest.raises(EndpointStoreError, match='400'):
            EndpointStore('test', **endpoint_store.kwargs)

    with mock.patch('requests.get', return_value=response):
        key = store.set([1, 2, 3], key='key')
        assert store.get(key) is None

    response.status_code = 401

    with mock.patch('requests.get', return_value=response):
        with pytest.raises(EndpointStoreError, match='401'):
            store.exists(key)

        with pytest.raises(EndpointStoreError, match='401'):
            store.get(key)

    with mock.patch('requests.post', return_value=response):
        with pytest.raises(EndpointStoreError, match='401'):
            store.evict(key)

        with pytest.raises(EndpointStoreError, match='401'):
            store.set([1, 2, 3], key='key')


def test_key_parse() -> None:
    with pytest.raises(ValueError, match='key'):
        EndpointStore._parse_key('a:b:c')

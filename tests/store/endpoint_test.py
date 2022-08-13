"""EndpointStore Unit Tests."""
from __future__ import annotations

import uuid
from unittest import mock

import pytest
import requests

from proxystore.endpoint.serve import MAX_CHUNK_LENGTH
from proxystore.store.endpoint import EndpointStore
from proxystore.store.endpoint import EndpointStoreError
from testing.compat import randbytes


def test_no_endpoints_provided() -> None:
    with pytest.raises(ValueError):
        EndpointStore('name', endpoints=[])


def test_no_endpoints_match(endpoint_store) -> None:
    with pytest.raises(EndpointStoreError, match='Failed to find'):
        EndpointStore(
            'name',
            endpoints=[str(uuid.uuid4())],
            proxystore_dir=endpoint_store.kwargs['proxystore_dir'],
        )


def test_no_endpoints_accessible(endpoint_store) -> None:
    response = requests.Response()
    response.status_code = 400

    with mock.patch('requests.get', return_value=response):
        with pytest.raises(EndpointStoreError, match='Failed to find'):
            EndpointStore('test', **endpoint_store.kwargs)


def test_endpoint_uuid_mismatch(endpoint_store) -> None:
    response = requests.Response()
    response.status_code = 200
    response.json = lambda: {'uuid': str(uuid.uuid4())}  # type: ignore

    with mock.patch('requests.get', return_value=response):
        with pytest.raises(EndpointStoreError, match='Failed to find'):
            EndpointStore('test', **endpoint_store.kwargs)


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
        key = store.set([1, 2, 3])
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
            store.set([1, 2, 3])


def test_chunked_requests(endpoint_store) -> None:
    store = EndpointStore(
        endpoint_store.name,
        **endpoint_store.kwargs,
        cache_size=0,
    )

    # Set to 2*chunk_size + 1 to force there to be two full size chunks
    # and one partial chunk
    data = randbytes((2 * MAX_CHUNK_LENGTH) + 1)
    key = store.set(data)

    assert store.get(key) == data

    store.close()

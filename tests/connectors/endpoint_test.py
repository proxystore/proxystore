from __future__ import annotations

import uuid
from unittest import mock

import pytest
import requests

from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.endpoint import EndpointConnectorError
from proxystore.endpoint.serve import MAX_CHUNK_LENGTH
from testing.compat import randbytes


def test_no_endpoints_provided() -> None:
    with pytest.raises(ValueError):
        EndpointConnector(endpoints=[])


def test_no_endpoints_match(endpoint_connector) -> None:
    with pytest.raises(EndpointConnectorError, match='Failed to find'):
        EndpointConnector(
            endpoints=[str(uuid.uuid4())],
            proxystore_dir=endpoint_connector.config()['proxystore_dir'],
        )


def test_no_endpoints_accessible(endpoint_connector) -> None:
    response = requests.Response()
    response.status_code = 400

    with mock.patch('requests.Session.get', return_value=response):
        with pytest.raises(EndpointConnectorError, match='Failed to find'):
            EndpointConnector.from_config(endpoint_connector.config())


def test_endpoint_uuid_mismatch(endpoint_connector) -> None:
    response = requests.Response()
    response.status_code = 200
    response.json = lambda: {'uuid': str(uuid.uuid4())}  # type: ignore

    with mock.patch('requests.Session.get', return_value=response):
        with pytest.raises(EndpointConnectorError, match='Failed to find'):
            EndpointConnector.from_config(endpoint_connector.config())


def test_bad_responses(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())

    response = requests.Response()
    response.status_code = 404

    with mock.patch('requests.Session.get', return_value=response):
        key = connector.put(b'value')
        assert connector.get(key) is None

    response.status_code = 401

    with mock.patch('requests.Session.get', return_value=response):
        with pytest.raises(EndpointConnectorError, match='401'):
            connector.exists(key)

        with pytest.raises(EndpointConnectorError, match='401'):
            connector.get(key)

    with mock.patch('requests.Session.post', return_value=response):
        with pytest.raises(EndpointConnectorError, match='401'):
            connector.evict(key)

        with pytest.raises(EndpointConnectorError, match='401'):
            connector.put(b'value')

    connector.close()


def test_chunked_requests(endpoint_connector) -> None:
    connector = EndpointConnector.from_config(endpoint_connector.config())

    # Set to 2*chunk_size + 1 to force there to be two full size chunks
    # and one partial chunk
    data = randbytes((2 * MAX_CHUNK_LENGTH) + 1)
    key = connector.put(data)

    assert connector.get(key) == data

    connector.close()

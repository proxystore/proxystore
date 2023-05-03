from __future__ import annotations

import uuid
from unittest import mock

import pytest
import requests

from proxystore.endpoint import client
from proxystore.endpoint.config import EndpointConfig


def test_basic_client_interaction(endpoint: EndpointConfig) -> None:
    address = f'http://{endpoint.host}:{endpoint.port}'
    key = str(uuid.uuid4())
    data = b'test'

    client.put(address, key, data)
    assert client.exists(address, key)
    assert client.get(address, key) == data

    client.evict(address, key)
    assert not client.exists(address, key)
    assert client.get(address, key) is None


def test_client_interaction_with_session(endpoint: EndpointConfig) -> None:
    address = f'http://{endpoint.host}:{endpoint.port}'
    key = str(uuid.uuid4())
    data = b'test'

    with requests.Session() as session:
        client.put(address, key, data, session=session)
        assert client.exists(address, key, session=session)
        assert client.get(address, key, session=session) == data

        client.evict(address, key, session=session)
        assert not client.exists(address, key, session=session)
        assert client.get(address, key, session=session) is None


def test_errors_raised() -> None:
    address = 'http://localhost:8539'
    key = 'abcd'

    response = requests.Response()
    response.status_code = 500

    with mock.patch('requests.post', return_value=response):
        with pytest.raises(requests.exceptions.RequestException):
            client.evict(address, key)

        with pytest.raises(requests.exceptions.RequestException):
            client.put(address, key, b'data')

    with mock.patch('requests.get', return_value=response):
        with pytest.raises(requests.exceptions.RequestException):
            client.exists(address, key)

        with pytest.raises(requests.exceptions.RequestException):
            client.get(address, key)

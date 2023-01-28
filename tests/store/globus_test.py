"""Globus Store Functionality Tests."""
from __future__ import annotations

import json
import os
import re
import uuid
from unittest import mock

import globus_sdk
import pytest

from proxystore.globus import GlobusAuthFileError
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.store.globus import GlobusStoreKey

EP1 = GlobusEndpoint(
    uuid='1',
    endpoint_path='/path',
    local_path='/path',
    host_regex='localhost',
)
EP2 = GlobusEndpoint(
    uuid='2',
    endpoint_path='/path',
    local_path='/path',
    host_regex='localhost',
)
EP3 = GlobusEndpoint(
    uuid='3',
    endpoint_path='/path',
    local_path='/path',
    host_regex='localhost',
)
EP4 = GlobusEndpoint(
    uuid='4',
    endpoint_path='/path',
    local_path='/path',
    host_regex=r'^\w{4}4$',
)
EP5 = GlobusEndpoint(
    uuid='1',
    endpoint_path='/path',
    local_path='/path',
    host_regex='localhost',
)


def test_globus_endpoint_objects() -> None:
    """Test GlobusEndpoint(s) Objects."""
    with pytest.raises(TypeError):
        GlobusEndpoint(
            uuid=1,  # type: ignore
            endpoint_path='1',
            local_path='1',
            host_regex='1',
        )
    with pytest.raises(TypeError):
        GlobusEndpoint(
            uuid='1',
            endpoint_path=1,  # type: ignore
            local_path='1',
            host_regex='1',
        )
    with pytest.raises(TypeError):
        GlobusEndpoint(
            uuid='1',
            endpoint_path='1',
            local_path=1,  # type: ignore
            host_regex='1',
        )
    with pytest.raises(TypeError):
        GlobusEndpoint(
            uuid='1',
            endpoint_path='1',
            local_path='1',
            host_regex=1,  # type: ignore
        )

    # GlobusEndpoint equality done by UUID
    assert EP1 != EP2
    assert EP1 == EP5

    # Check must pass at least one endpoint
    with pytest.raises(ValueError):
        GlobusEndpoints([])

    # Check not able to pass multiple endpoints same UUID
    with pytest.raises(ValueError):
        GlobusEndpoints([EP1, EP5])

    eps = GlobusEndpoints([EP1, EP2, EP3, EP4])
    assert len(eps) == 4

    assert eps[EP1.uuid] == EP1
    with pytest.raises(KeyError):
        assert eps['-1']

    for x, y in zip([EP1, EP2], eps):
        assert x == y

    assert eps.get_by_host('localhost') == EP1
    assert eps.get_by_host('host4') == EP4
    with pytest.raises(ValueError):
        eps.get_by_host('host2_')
    with pytest.raises(ValueError):
        eps.get_by_host('host3')


def test_globus_endpoints_from_json() -> None:
    """Test GlobusEndpoints from JSON file."""
    data = {
        'UUID1': {
            'endpoint_path': '/~/',
            'local_path': '/home/user1/',
            'host_regex': 'host1',
        },
        'UUID2': {
            'endpoint_path': '/~/',
            'local_path': '/home/user2/',
            'host_regex': 'host2',
        },
    }
    filepath = f'/tmp/endpoints-{uuid.uuid4()}.json'
    with open(filepath, 'w') as f:
        f.write(json.dumps(data))

    endpoints = GlobusEndpoints.from_json(filepath)

    os.remove(filepath)

    assert len(endpoints) == 2
    assert endpoints['UUID1'].endpoint_path == '/~/'
    assert endpoints['UUID1'].local_path == '/home/user1/'
    assert endpoints['UUID1'].host_regex == 'host1'
    assert endpoints['UUID2'].endpoint_path == '/~/'
    assert endpoints['UUID2'].local_path == '/home/user2/'
    assert endpoints['UUID2'].host_regex == 'host2'


def test_globus_endpoints_from_dict() -> None:
    """Test GlobusEndpoints from JSON file."""
    data = {
        'UUID1': {
            'endpoint_path': '/~/',
            'local_path': '/home/user1/',
            'host_regex': 'host1',
        },
        'UUID2': {
            'endpoint_path': '/~/',
            'local_path': '/home/user2/',
            'host_regex': 'host2',
        },
    }
    endpoints = GlobusEndpoints.from_dict(data)
    assert endpoints.dict() == data

    # Ensure Patterns are converted to strings in .dict()
    data['UUID1']['host_regex'] = re.compile('host1')  # type: ignore
    endpoints = GlobusEndpoints.from_dict(data)
    assert isinstance(endpoints.dict()['UUID1']['host_regex'], str)


def test_globus_store_init(globus_store) -> None:
    """Test GlobusStore Initialization."""
    eps = GlobusEndpoints([EP1, EP2])

    GlobusStore('globus', endpoints=[EP1, EP2])

    s1 = GlobusStore('globus1', endpoints=[EP1, EP2])
    s2 = GlobusStore('globus2', endpoints=eps)
    s3 = GlobusStore('globus3', endpoints=eps.dict())
    assert s1.kwargs == s2.kwargs == s3.kwargs

    with pytest.raises(ValueError):
        # Invalid endpoint type
        GlobusStore('globus', endpoints=None)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        # Too many endpoints
        GlobusStore('globus', endpoints=[EP1, EP2, EP3])

    with pytest.raises(ValueError):
        # Not enough endpoints
        GlobusStore('globus', endpoints=[EP1])


def test_globus_store_internals(globus_store) -> None:
    """Test GlobusStore internal mechanisms."""
    store = GlobusStore('globus', **globus_store.kwargs)

    class PatchedError(globus_sdk.TransferAPIError):
        def __init__(self, status: int):
            self.http_status = status

    def _http_error(status: int):
        def _error(*args, **kwargs) -> None:
            raise PatchedError(status)

        return _error

    store._transfer_client.get_task = _http_error(400)  # type: ignore
    assert not store._validate_task_id('uuid')
    assert not store.exists(GlobusStoreKey('fake', 'fake'))

    store._transfer_client.get_task = _http_error(401)  # type: ignore
    with pytest.raises(globus_sdk.TransferAPIError):
        store._validate_task_id('uuid')

    def _fail_wait(*args, **kwargs) -> bool:
        return False

    store._transfer_client.task_wait = _fail_wait  # type: ignore
    with pytest.raises(RuntimeError):
        store._wait_on_tasks('1234')


def test_globus_store_set_batch_type_error(globus_store) -> None:
    """Test GlobusStore internal mechanisms."""
    store = GlobusStore('globus', **globus_store.kwargs)

    objs = [1, 2, 3]
    with pytest.raises(TypeError):
        store.set_batch(objs, serializer=lambda s: s)


def test_get_filepath(globus_store) -> None:
    """Test GlobusStore filepath building."""
    endpoints = GlobusEndpoints(
        [
            GlobusEndpoint(
                uuid='EP1UUID',
                endpoint_path='/~/',
                local_path='/tmp/proxystore-test-1',
                host_regex='localhost',
            ),
            GlobusEndpoint(
                uuid='EP2UUID',
                endpoint_path='/~/',
                local_path='/tmp/proxystore-test-2',
                host_regex='localhost',
            ),
        ],
    )

    store = GlobusStore(globus_store.name, endpoints=endpoints)

    filename = 'test_file'
    for endpoint in endpoints:
        expected_path = os.path.join(endpoint.local_path, filename)
        assert store._get_filepath(filename, endpoint) == expected_path


def test_expand_user_path(globus_store) -> None:
    """Test GlobusStore expands user path."""
    store_dir = '.cache/proxystore_cache'
    short_path = os.path.join('~', store_dir)
    full_path = os.path.join(os.path.expanduser('~'), store_dir)

    ep1 = GlobusEndpoint(
        uuid='EP1UUID',
        endpoint_path='/~/',
        local_path=short_path,
        host_regex='localhost',
    )
    ep2 = GlobusEndpoint(
        uuid='EP2UUID',
        endpoint_path='/~/',
        local_path=full_path,
        host_regex='localhost',
    )

    store = GlobusStore('globus', endpoints=[ep1, ep2])

    filename = 'test_file'
    assert '~' not in store._get_filepath(filename, ep1)
    assert store._get_filepath(filename, ep1) == store._get_filepath(
        filename,
        ep2,
    )


def test_globus_auth_not_done() -> None:
    """Test Globus auth missing during Store init."""
    with mock.patch(
        'proxystore.store.globus.get_proxystore_authorizer',
        side_effect=GlobusAuthFileError,
    ):
        with pytest.raises(GlobusAuthFileError, match='Complete the'):
            GlobusStore('store', endpoints=[EP1, EP2])


def test_globus_store_key_equality() -> None:
    """Test GlobusStoreKey custom equality."""
    key = GlobusStoreKey('a', 'b')
    assert key == GlobusStoreKey('a', 'b')
    assert key == ('a', 'b')
    assert key != ('b', 'b')
    assert key == ('a', 'c')
    assert key != 'a'

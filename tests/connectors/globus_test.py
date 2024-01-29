from __future__ import annotations

import json
import os
import re
import uuid
from unittest import mock

import globus_sdk
import pytest

from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.globus import GlobusEndpoint
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.globus import GlobusKey

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


def test_reprs() -> None:
    assert isinstance(str(EP1), str)
    assert isinstance(str(GlobusEndpoints([EP1, EP2])), str)


def test_globus_endpoint_objects() -> None:
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


def test_globus_connector_init(globus_connector) -> None:
    eps = GlobusEndpoints([EP1, EP2])

    s1 = GlobusConnector(endpoints=[EP1, EP2])
    s2 = GlobusConnector(endpoints=eps)
    s3 = GlobusConnector(endpoints=eps.dict())
    assert s1.config() == s2.config() == s3.config()

    with pytest.raises(ValueError):
        # Invalid endpoint type
        GlobusConnector(endpoints=None)  # type: ignore[arg-type]

    with pytest.raises(
        ValueError,
        match='At least two Globus endpoints are required.',
    ):
        GlobusConnector(endpoints=[EP1])


def test_globus_connector_internals(globus_connector) -> None:
    connector = GlobusConnector.from_config(globus_connector.config())

    class PatchedError(globus_sdk.TransferAPIError):
        def __init__(self, status: int):
            self.http_status = status

    def _http_error(status: int):
        def _error(*args, **kwargs) -> None:
            raise PatchedError(status)

        return _error

    connector._transfer_client.get_task = _http_error(400)  # type: ignore
    assert not connector._validate_task_id('uuid')
    assert not connector.exists(GlobusKey('fake', 'fake'))

    connector._transfer_client.get_task = _http_error(401)  # type: ignore
    with pytest.raises(globus_sdk.TransferAPIError):
        connector._validate_task_id('uuid')

    def _fail_wait(*args, **kwargs) -> bool:
        return False

    connector._transfer_client.task_wait = _fail_wait  # type: ignore
    with pytest.raises(RuntimeError):
        connector._wait_on_tasks('1234')


def test_get_filepath(globus_connector) -> None:
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

    connector = GlobusConnector(endpoints=endpoints)

    filename = 'test_file'
    for endpoint in endpoints:
        expected_path = os.path.join(endpoint.local_path, filename)
        assert connector._get_filepath(filename, endpoint) == expected_path


def test_expand_user_path(globus_connector) -> None:
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

    connector = GlobusConnector(endpoints=[ep1, ep2])

    filename = 'test_file'
    assert '~' not in connector._get_filepath(filename, ep1)
    assert connector._get_filepath(filename, ep1) == connector._get_filepath(
        filename,
        ep2,
    )


def test_globus_connector_key_equality() -> None:
    key = GlobusKey('a', 'b')
    assert key == GlobusKey('a', 'b')
    assert key == ('a', 'b')
    assert key != ('b', 'b')
    assert key == ('a', 'c')
    assert key != 'a'


@pytest.mark.parametrize(
    ('clear_default', 'clear_override', 'should_exist'),
    ((True, None, False), (False, True, False), (False, False, True)),
)
def test_delete_local_paths_on_close(
    clear_default: bool,
    clear_override: bool | None,
    should_exist: bool,
) -> None:
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

    with mock.patch(
        'proxystore.connectors.globus.get_transfer_client_flow',
    ), mock.patch(
        'proxystore.connectors.globus._submit_transfer_action',
        return_value={'task_id': 'ABCD'},
    ) as mocked:
        connector = GlobusConnector(endpoints=endpoints, clear=clear_default)
        connector.close(clear=clear_override)

    if should_exist:
        assert mocked.call_count == 0
    else:
        assert mocked.call_count == len(endpoints)

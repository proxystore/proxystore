"""Testing fixtures for connector implementations."""
from __future__ import annotations

import os
import random
from typing import Any
from typing import Generator
from unittest import mock

import pytest

from proxystore.connectors.connector import Connector
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.globus import GlobusEndpoint
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.local import LocalConnector
from proxystore.connectors.local import LocalKey
from proxystore.connectors.multi import MultiConnector
from proxystore.connectors.multi import Policy
from proxystore.connectors.redis import RedisConnector
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.utils import hostname
from testing.mocked.globus import MockDeleteData
from testing.mocked.globus import MockTransferClient
from testing.mocked.globus import MockTransferData
from testing.mocked.redis import MockStrictRedis

FIXTURE_LIST = [
    'endpoint_connector',
    'globus_connector',
    'file_connector',
    'local_connector',
    'multi_connector',
    'redis_connector',
]
MOCK_REDIS_CACHE: dict[str, Any] = {}


@pytest.fixture(scope='session')
def endpoint_connector(
    endpoint: EndpointConfig,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[EndpointConnector, None, None]:
    """EndpointConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('endpoint-connector-fixture')
    tmp_dir = str(tmp_path)
    endpoint_dir = os.path.join(tmp_dir, endpoint.name)
    write_config(endpoint, endpoint_dir)
    connector = EndpointConnector(
        endpoints=[endpoint.uuid],
        proxystore_dir=tmp_dir,
    )
    yield connector
    connector.close()


@pytest.fixture(scope='session')
def globus_connector(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[GlobusConnector, None, None]:
    """GlobusConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('globus-connector-fixture')
    endpoints = GlobusEndpoints(
        [
            GlobusEndpoint(
                uuid='EP1UUID',
                endpoint_path='/~/',
                local_path=str(tmp_path),
                host_regex=hostname(),
            ),
            GlobusEndpoint(
                uuid='EP2UUID',
                endpoint_path='/~/',
                local_path=str(tmp_path),
                host_regex=hostname(),
            ),
        ],
    )

    with mock.patch(
        'proxystore.store.globus.get_proxystore_authorizer',
    ), mock.patch(
        'globus_sdk.TransferClient',
        MockTransferClient,
    ), mock.patch(
        'globus_sdk.DeleteData',
        MockDeleteData,
    ), mock.patch(
        'globus_sdk.TransferData',
        MockTransferData,
    ):
        connector = GlobusConnector(endpoints=endpoints)
        yield connector
        connector.close()


@pytest.fixture(scope='session')
def file_connector(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[FileConnector, None, None]:
    """FileConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('file-connector-fixture')
    connector = FileConnector(str(tmp_path))
    yield connector
    connector.close()


@pytest.fixture(scope='session')
def local_connector() -> Generator[LocalConnector, None, None]:
    """LocalConnector fixture."""
    store_dict: dict[LocalKey, bytes] = {}
    connector = LocalConnector(store_dict)
    yield connector
    connector.close()


@pytest.fixture(scope='session')
def multi_connector() -> Generator[MultiConnector, None, None]:
    """MultiConnector fixture."""
    store_dict: dict[LocalKey, bytes] = {}
    local = LocalConnector(store_dict)
    policy = Policy(priority=0)
    connectors = {'local': (local, policy)}
    connector = MultiConnector(connectors=connectors)  # type: ignore[arg-type]
    yield connector
    connector.close()


@pytest.fixture(scope='session')
def redis_connector() -> Generator[RedisConnector, None, None]:
    """RedisConnector fixture."""
    redis_host = 'localhost'
    redis_port = random.randint(5500, 5999)

    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE
    MOCK_REDIS_CACHE = {}

    def create_mocked_redis(*args: Any, **kwargs: Any) -> MockStrictRedis:
        return MockStrictRedis(MOCK_REDIS_CACHE, *args, **kwargs)

    with mock.patch('redis.StrictRedis', side_effect=create_mocked_redis):
        connector = RedisConnector(hostname=redis_host, port=redis_port)
        yield connector
        connector.close()


@pytest.fixture(scope='session', params=FIXTURE_LIST)
def connectors(request) -> Connector[Any]:
    """Parameterized fixture that yields all Connector implementations."""
    return request.getfixturevalue(request.param)

"""Testing fixtures for connector implementations."""

from __future__ import annotations

import os
import random
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors import file
from proxystore.connectors import globus
from proxystore.connectors import local
from proxystore.connectors import multi
from proxystore.connectors import redis
from proxystore.connectors import zmq
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.protocols import Connector
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.utils.environment import hostname
from testing.mocked.globus import MockDeleteData
from testing.mocked.globus import MockTransferClient
from testing.mocked.globus import MockTransferData
from testing.mocked.redis import MockStrictRedis
from testing.utils import open_port

FIXTURE_LIST = [
    'daos_connector',
    'endpoint_connector',
    'globus_connector',
    'file_connector',
    'local_connector',
    'multi_connector',
    'redis_connector',
    'zmq_connector',
]
MOCK_REDIS_CACHE: dict[str, Any] = {}


@pytest.fixture(scope='session')
def daos_connector() -> Generator[Connector[Any], None, None]:
    """DAOSConnector fixture using the mocked PyDAOS."""
    # Imported here because tests/conftest.py replaces pydaos with a mocked
    # version after this module is imported.
    from proxystore.connectors.daos import DAOSConnector

    with DAOSConnector(
        pool='test-pool',
        container='test-container',
        namespace='test-namespace',
    ) as connector:
        yield connector


@pytest.fixture(scope='session')
def endpoint_connector(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> Generator[Connector[Any], None, None]:
    """EndpointConnector fixture."""
    with EndpointConnector(
        endpoints=[endpoint.uuid],
        proxystore_dir=os.path.dirname(endpoint_dir.path),
    ) as connector:
        yield connector


@pytest.fixture(scope='session')
def globus_connector(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[Connector[Any], None, None]:
    """GlobusConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('globus-connector-fixture')
    endpoints = globus.GlobusEndpoints(
        [
            globus.GlobusEndpoint(
                uuid='EP1UUID',
                endpoint_path='/~/',
                local_path=str(tmp_path),
                host_regex=hostname(),
            ),
            globus.GlobusEndpoint(
                uuid='EP2UUID',
                endpoint_path='/~/',
                local_path=str(tmp_path),
                host_regex=hostname(),
            ),
        ],
    )

    with (
        mock.patch(
            'globus_sdk.DeleteData',
            MockDeleteData,
        ),
        mock.patch(
            'globus_sdk.TransferData',
            MockTransferData,
        ),
        mock.patch(
            'proxystore.connectors.globus.get_transfer_client',
            MockTransferClient,
        ),
        globus.GlobusConnector(
            endpoints=endpoints,
            buffering=0,
        ) as connector,
    ):
        yield connector


@pytest.fixture(scope='session')
def file_connector(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[Connector[Any], None, None]:
    """FileConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('file-connector-fixture')
    with file.FileConnector(str(tmp_path), buffering=0) as connector:
        yield connector


@pytest.fixture(scope='session')
def local_connector() -> Generator[Connector[Any], None, None]:
    """LocalConnector fixture."""
    with local.LocalConnector() as connector:
        yield connector


@pytest.fixture(scope='session')
def multi_connector() -> Generator[Connector[Any], None, None]:
    """MultiConnector fixture."""
    connector_policy = (local.LocalConnector(), multi.Policy(priority=0))
    with multi.MultiConnector({'local': connector_policy}) as connector:
        yield connector


@pytest.fixture(scope='session')
def redis_connector() -> Generator[Connector[Any], None, None]:
    """RedisConnector fixture."""
    redis_host = 'localhost'
    redis_port = random.randint(5500, 5999)

    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE  # noqa: PLW0603
    MOCK_REDIS_CACHE = {}

    def create_mocked_redis(*args: Any, **kwargs: Any) -> MockStrictRedis:
        return MockStrictRedis(MOCK_REDIS_CACHE, *args, **kwargs)

    with (
        mock.patch('redis.StrictRedis', side_effect=create_mocked_redis),
        redis.RedisConnector(redis_host, redis_port) as connector,
    ):
        yield connector


@pytest.fixture(scope='session')
def zmq_connector() -> Generator[Connector[Any], None, None]:
    """ZeroMQConnector fixture."""
    connector = zmq.ZeroMQConnector(open_port(), address='127.0.0.1')
    yield connector
    connector.close(kill_server=True)


@pytest.fixture(scope='session', params=FIXTURE_LIST)
def connectors(request) -> Generator[Connector[Any], None, None]:
    """Parameterized fixture that returns all Connector implementations."""
    connector = request.getfixturevalue(request.param)

    with mock.patch.object(
        connector,
        'close',
        side_effect=RuntimeError(
            'Tests using connectors fixtures should not call '
            'close() on the yielded connector instance.',
        ),
    ):
        yield connector

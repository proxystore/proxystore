"""Testing fixtures for connector implementations."""
from __future__ import annotations

import contextlib
import importlib.util
import os
import random
from typing import Any
from typing import Callable
from typing import ContextManager
from typing import Generator
from typing import NamedTuple
from unittest import mock

import pytest

from proxystore.connectors.connector import Connector
from proxystore.connectors.dim.margo import MargoConnector
from proxystore.connectors.dim.ucx import reset_ucp
from proxystore.connectors.dim.ucx import UCXConnector
from proxystore.connectors.dim.zmq import ZeroMQConnector
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
from testing.mocking import mock_multiprocessing
from testing.utils import open_port

FIXTURE_LIST = [
    'endpoint_connector',
    'globus_connector',
    'file_connector',
    'local_connector',
    'multi_connector',
    'redis_connector',
    'margo_connector',
    'ucx_connector',
    'zmq_connector',
]
MOCK_REDIS_CACHE: dict[str, Any] = {}


class ConnectorInfo(NamedTuple):
    """Info needed to initialize an arbitrary Connector."""

    type: type[Connector[Any]]
    kwargs: dict[str, Any]
    # ctx is a callable that takes no arguments and returns a context
    # manager designed for enabling easy mocking without
    # having to mock at the session level. I.e., all of the store fixtures
    # here are session scoped so mocking in the fixture would have the mock
    # affect ALL tests. Instead, tests/fixtures can invoke the context as
    # needed when creating an instance of the store. Not all mocks need to be
    # in context, just the ones that effect objects that may be needed in
    # unmocked form by other tests.
    ctx: Callable[[], ContextManager[None]]


@pytest.fixture(scope='session')
def endpoint_connector(
    endpoint: EndpointConfig,
    tmp_path_factory: pytest.TempPathFactory,
) -> ConnectorInfo:
    """EndpointConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('endpoint-connector-fixture')
    tmp_dir = str(tmp_path)
    endpoint_dir = os.path.join(tmp_dir, endpoint.name)
    write_config(endpoint, endpoint_dir)

    return ConnectorInfo(
        EndpointConnector,
        {'endpoints': [endpoint.uuid], 'proxystore_dir': tmp_dir},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session')
def globus_connector(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[ConnectorInfo, None, None]:
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
        'proxystore.connectors.globus.get_proxystore_authorizer',
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
        yield ConnectorInfo(
            GlobusConnector,
            {'endpoints': endpoints},
            contextlib.nullcontext,
        )


@pytest.fixture(scope='session')
def file_connector(
    tmp_path_factory: pytest.TempPathFactory,
) -> ConnectorInfo:
    """FileConnector fixture."""
    tmp_path = tmp_path_factory.mktemp('file-connector-fixture')
    return ConnectorInfo(
        FileConnector,
        {'store_dir': str(tmp_path)},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session')
def local_connector() -> ConnectorInfo:
    """LocalConnector fixture."""
    store_dict: dict[LocalKey, bytes] = {}
    return ConnectorInfo(
        LocalConnector,
        {'store_dict': store_dict},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session')
def multi_connector() -> ConnectorInfo:
    """MultiConnector fixture."""
    store_dict: dict[LocalKey, bytes] = {}
    local = LocalConnector(store_dict)
    policy = Policy(priority=0)
    connectors = {'local': (local, policy)}
    return ConnectorInfo(
        MultiConnector,
        {'connectors': connectors},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session')
def redis_connector() -> Generator[ConnectorInfo, None, None]:
    """RedisConnector fixture."""
    redis_host = 'localhost'
    redis_port = random.randint(5500, 5999)

    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE
    MOCK_REDIS_CACHE = {}

    def create_mocked_redis(*args: Any, **kwargs: Any) -> MockStrictRedis:
        return MockStrictRedis(MOCK_REDIS_CACHE, *args, **kwargs)

    with mock.patch('redis.StrictRedis', side_effect=create_mocked_redis):
        yield ConnectorInfo(
            RedisConnector,
            {'hostname': redis_host, 'port': redis_port},
            contextlib.nullcontext,
        )


@pytest.fixture(scope='session')
def margo_connector() -> ConnectorInfo:
    """MargoConnector fixture."""
    host = '127.0.0.1'
    port = open_port()
    protocol = 'tcp'

    ctx: Callable[[], ContextManager[None]] = contextlib.nullcontext
    margo_spec = importlib.util.find_spec('pymargo')

    if (  # pragma: no branch
        margo_spec is not None and 'mocked' in margo_spec.name
    ):
        ctx = mock_multiprocessing

    return ConnectorInfo(
        MargoConnector,
        {'protocol': protocol, 'interface': host, 'port': port},
        ctx,
    )


@pytest.fixture(scope='session')
def ucx_connector() -> Generator[ConnectorInfo, None, None]:
    """UCXConnector fixture."""
    port = open_port()

    ctx: Callable[[], ContextManager[None]] = contextlib.nullcontext
    ucp_spec = importlib.util.find_spec('ucp')

    if ucp_spec is not None and 'mocked' in ucp_spec.name:  # pragma: no branch
        ctx = mock_multiprocessing

    yield ConnectorInfo(
        UCXConnector,
        {'interface': '127.0.0.1', 'port': port},
        ctx,
    )

    if (
        ucp_spec is not None and 'mocked' not in ucp_spec.name
    ):  # pragma: no cover
        reset_ucp()


@pytest.fixture(scope='session')
def zmq_connector() -> ConnectorInfo:
    """ZeroMQ store fixture."""
    port = open_port()

    return ConnectorInfo(
        ZeroMQConnector,
        {'interface': 'localhost', 'port': port},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session', params=FIXTURE_LIST)
def connectors(request) -> Generator[Connector[Any], None, None]:
    """Parameterized fixture that yields all Connector implementations."""
    connector_info = request.getfixturevalue(request.param)

    with connector_info.type(**connector_info.kwargs) as connector:
        yield connector

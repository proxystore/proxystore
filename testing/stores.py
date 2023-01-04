"""Mocking utilities for Store tests."""
from __future__ import annotations

import contextlib
import importlib.util
import os
import random
import shutil
import uuid
from typing import Any
from typing import Callable
from typing import ContextManager
from typing import Generator
from typing import NamedTuple
from typing import Tuple
from typing import TypeVar
from unittest import mock

import pytest

from proxystore import utils
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.store.base import Store
from proxystore.store.dim.margo import MargoStore
from proxystore.store.dim.margo import MargoStoreKey
from proxystore.store.dim.ucx import reset_ucp
from proxystore.store.dim.ucx import UCXStore
from proxystore.store.dim.ucx import UCXStoreKey
from proxystore.store.dim.websockets import WebsocketStore
from proxystore.store.dim.websockets import WebsocketStoreKey
from proxystore.store.endpoint import EndpointStore
from proxystore.store.endpoint import EndpointStoreKey
from proxystore.store.file import FileStore
from proxystore.store.file import FileStoreKey
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.store.globus import GlobusStoreKey
from proxystore.store.local import LocalStore
from proxystore.store.local import LocalStoreKey
from proxystore.store.redis import RedisStore
from proxystore.store.redis import RedisStoreKey
from testing.mocked.globus import MockDeleteData
from testing.mocked.globus import MockTransferClient
from testing.mocked.globus import MockTransferData
from testing.mocked.redis import MockStrictRedis
from testing.mocking import mock_multiprocessing
from testing.utils import open_port

FIXTURE_LIST = [
    'local_store',
    'file_store',
    'redis_store',
    'globus_store',
    'endpoint_store',
    'margo_store',
    'ucx_store',
    'websocket_store',
]

MOCK_REDIS_CACHE: dict[str, Any] = {}

KeyT = TypeVar('KeyT', bound=NamedTuple)


class StoreInfo(NamedTuple):
    """Info needed to initialize an arbitrary Store."""

    type: type[Store[Any]]
    name: str
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


StoreFixtureType = Tuple[Store[Any], StoreInfo]


@pytest.fixture(scope='session')
def local_store() -> Generator[StoreInfo, None, None]:
    """Local Store fixture."""
    store_dict: dict[str, bytes] = {}
    yield StoreInfo(
        LocalStore,
        'local',
        {'store_dict': store_dict},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session')
def file_store() -> Generator[StoreInfo, None, None]:
    """File Store fixture."""
    file_dir = f'/tmp/proxystore-test-{uuid.uuid4()}'
    yield StoreInfo(
        FileStore,
        'file',
        {'store_dir': file_dir},
        contextlib.nullcontext,
    )
    if os.path.exists(file_dir):  # pragma: no cover
        shutil.rmtree(file_dir)


@pytest.fixture(scope='session')
def redis_store() -> Generator[StoreInfo, None, None]:
    """Redis Store fixture."""
    redis_host = 'localhost'
    redis_port = random.randint(5500, 5999)

    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE
    MOCK_REDIS_CACHE = {}

    def create_mocked_redis(*args: Any, **kwargs: Any) -> MockStrictRedis:
        return MockStrictRedis(MOCK_REDIS_CACHE, *args, **kwargs)

    with mock.patch('redis.StrictRedis', side_effect=create_mocked_redis):
        yield StoreInfo(
            RedisStore,
            'redis',
            {'hostname': redis_host, 'port': redis_port},
            contextlib.nullcontext,
        )


@pytest.fixture(scope='session')
def globus_store() -> Generator[StoreInfo, None, None]:
    """Globus Store fixture."""
    file_dir = f'/tmp/proxystore-test-{uuid.uuid4()}'
    endpoints = GlobusEndpoints(
        [
            GlobusEndpoint(
                uuid='EP1UUID',
                endpoint_path='/~/',
                local_path=file_dir,
                host_regex=utils.hostname(),
            ),
            GlobusEndpoint(
                uuid='EP2UUID',
                endpoint_path='/~/',
                local_path=file_dir,
                host_regex=utils.hostname(),
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
        yield StoreInfo(
            GlobusStore,
            'globus',
            {'endpoints': endpoints},
            contextlib.nullcontext,
        )

    if os.path.exists(file_dir):  # pragma: no cover
        shutil.rmtree(file_dir)


@pytest.fixture(scope='session')
def endpoint_store(
    endpoint: EndpointConfig,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[StoreInfo, None, None]:
    """Endpoint Store fixture."""
    tmp_path = tmp_path_factory.mktemp('endpoint-store-fixture')
    tmp_dir = str(tmp_path)
    endpoint_dir = os.path.join(tmp_dir, endpoint.name)
    write_config(endpoint, endpoint_dir)

    yield StoreInfo(
        EndpointStore,
        'endpoint',
        {'endpoints': [endpoint.uuid], 'proxystore_dir': tmp_dir},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session')
def ucx_store() -> Generator[StoreInfo, None, None]:
    """UCX Store fixture."""
    port = open_port()

    ctx: Callable[[], ContextManager[None]] = contextlib.nullcontext
    ucp_spec = importlib.util.find_spec('ucp')

    if ucp_spec is not None and 'mocked' in ucp_spec.name:  # pragma: no branch
        ctx = mock_multiprocessing

    yield StoreInfo(
        UCXStore,
        'ucx',
        {'interface': '127.0.0.1', 'port': port},
        ctx,
    )

    if (
        ucp_spec is not None and 'mocked' not in ucp_spec.name
    ):  # pragma: no cover
        reset_ucp()


@pytest.fixture(scope='session')
def margo_store() -> Generator[StoreInfo, None, None]:
    """Margo Store fixture."""
    host = '127.0.0.1'
    port = open_port()
    protocol = 'tcp'

    ctx: Callable[[], ContextManager[None]] = contextlib.nullcontext
    margo_spec = importlib.util.find_spec('pymargo')

    if (  # pragma: no branch
        margo_spec is not None and 'mocked' in margo_spec.name
    ):
        ctx = mock_multiprocessing

    yield StoreInfo(
        MargoStore,
        'margo',
        {'protocol': protocol, 'interface': host, 'port': port},
        ctx,
    )


@pytest.fixture(scope='session')
def websocket_store() -> Generator[StoreInfo, None, None]:
    """Websocket store fixture."""
    port = open_port()

    yield StoreInfo(
        WebsocketStore,
        'websocket',
        {'interface': 'localhost', 'port': port},
        contextlib.nullcontext,
    )


@pytest.fixture(scope='session', params=FIXTURE_LIST)
def store_implementation(
    request,
) -> Generator[StoreFixtureType, None, None]:
    """Parameterized fixture that yields all Store implementations."""
    store_info = request.getfixturevalue(request.param)

    with store_info.ctx():
        store = store_info.type(
            store_info.name,
            cache_size=0,
            **store_info.kwargs,
        )

    with mock.patch.object(
        store,
        'close',
        side_effect=RuntimeError(
            'Tests using the store_implementation fixture should not call '
            'close() on the yielded Store instance.',
        ),
    ):
        yield store, store_info

    with store_info.ctx():
        store.close()


def missing_key(store: Store[Any]) -> NamedTuple:
    """Generate a random key that is valid for the store type."""
    if isinstance(store, EndpointStore):
        return EndpointStoreKey(str(uuid.uuid4()), str(uuid.uuid4()))
    elif isinstance(store, FileStore):
        return FileStoreKey(str(uuid.uuid4()))
    elif isinstance(store, GlobusStore):
        return GlobusStoreKey(str(uuid.uuid4()), str(uuid.uuid4()))
    elif isinstance(store, LocalStore):
        return LocalStoreKey(str(uuid.uuid4()))
    elif isinstance(store, RedisStore):
        return RedisStoreKey(str(uuid.uuid4()))
    elif isinstance(store, MargoStore):
        return MargoStoreKey(
            str(uuid.uuid4()),
            1,
            f'tcp://{store.kwargs["interface"]}:{store.kwargs["port"]}',
        )
    elif isinstance(store, UCXStore):
        return UCXStoreKey(
            str(uuid.uuid4()),
            1,
            f'localhost:{store.kwargs["port"]}',
        )
    elif isinstance(store, WebsocketStore):
        return WebsocketStoreKey(
            str(uuid.uuid4()),
            1,
            f'ws://localhost:{store.kwargs["port"]}',
        )
    else:
        raise AssertionError(f'Unsupported store type {type(store).__name__}')

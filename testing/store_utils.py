"""Mocking utilities for Store tests."""
from __future__ import annotations

import os
import random
import shutil
import socket
import uuid
from typing import Any
from typing import Generator
from typing import NamedTuple
from typing import TypeVar
from unittest import mock

import globus_sdk
import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.store.base import Store
from proxystore.store.dim.margo import MargoStore
from proxystore.store.dim.margo import MargoStoreKey
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
from testing.endpoint import launch_endpoint
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


class MockTransferData(globus_sdk.TransferData):
    """Mock the Globus TransferData."""

    def __init__(self, *args, **kwargs):
        """Init MockTransferData."""
        pass

    def __setitem__(self, key, item):
        """Set item."""
        self.__dict__[key] = item

    def add_item(
        self,
        source_path: str,
        destination_path: str,
        **kwargs: Any,
    ) -> None:
        """Add item."""
        assert isinstance(source_path, str)
        assert isinstance(destination_path, str)


class MockDeleteData(globus_sdk.DeleteData):
    """Mock the Globus DeleteData."""

    def __init__(self, *args, **kwargs):
        """Init MockDeleteData."""
        pass

    def __setitem__(self, key, item):
        """Set item."""
        self.__dict__[key] = item

    def add_item(self, path: str, **kwargs: Any) -> None:
        """Add item."""
        assert isinstance(path, str)


class MockTransferClient:
    """Mock the Globus TransferClient."""

    def __init__(self, *args, **kwargs):
        """Init MockTransferClient."""
        pass

    def get_task(self, task_id: str) -> Any:
        """Get task."""
        assert isinstance(task_id, str)
        return None

    def submit_delete(self, delete_data: MockDeleteData) -> dict[str, str]:
        """Submit DeleteData."""
        assert isinstance(delete_data, MockDeleteData)
        return {'task_id': str(uuid.uuid4())}

    def submit_transfer(
        self,
        transfer_data: MockTransferData,
    ) -> dict[str, str]:
        """Submit TransferData."""
        assert isinstance(transfer_data, MockTransferData)
        return {'task_id': str(uuid.uuid4())}

    def task_wait(self, task_id: str, **kwargs: Any) -> bool:
        """Wait on tasks."""
        assert isinstance(task_id, str)
        return True


class MockStrictRedis:
    """Mock StrictRedis."""

    def __init__(self, *args, **kwargs):
        """Init MockStrictRedis."""
        # Use global MOCK_REDIS_CACHE so different RedisStores access the
        # same data
        self.data = MOCK_REDIS_CACHE

    def delete(self, key: str) -> None:
        """Delete key."""
        if key in self.data:
            del self.data[key]

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data

    def get(self, key: str) -> Any:
        """Get value with key."""
        if key in self.data:
            return self.data[key]
        return None

    def set(self, key: str, value: str | bytes | int | float) -> None:
        """Set value in MockStrictRedis."""
        self.data[key] = value


class StoreInfo(NamedTuple):
    """Info needed to initialize an arbitrary Store."""

    type: type[Store[Any]]
    name: str
    kwargs: dict[str, Any]


@pytest.fixture
def local_store() -> Generator[StoreInfo, None, None]:
    """Local Store fixture."""
    store_dict: dict[str, bytes] = {}
    yield StoreInfo(LocalStore, 'local', {'store_dict': store_dict})


@pytest.fixture
def file_store() -> Generator[StoreInfo, None, None]:
    """File Store fixture."""
    file_dir = f'/tmp/proxystore-test-{uuid.uuid4()}'
    yield StoreInfo(FileStore, 'file', {'store_dir': file_dir})
    if os.path.exists(file_dir):  # pragma: no cover
        shutil.rmtree(file_dir)


@pytest.fixture
def redis_store() -> Generator[StoreInfo, None, None]:
    """Redis Store fixture."""
    redis_host = 'localhost'
    redis_port = random.randint(5500, 5999)

    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE
    MOCK_REDIS_CACHE = {}

    with mock.patch('redis.StrictRedis', side_effect=MockStrictRedis):
        yield StoreInfo(
            RedisStore,
            'redis',
            {'hostname': redis_host, 'port': redis_port},
        )


@pytest.fixture
def globus_store() -> Generator[StoreInfo, None, None]:
    """Globus Store fixture."""
    file_dir = f'/tmp/proxystore-test-{uuid.uuid4()}'
    endpoints = GlobusEndpoints(
        [
            GlobusEndpoint(
                uuid='EP1UUID',
                endpoint_path='/~/',
                local_path=file_dir,
                host_regex=socket.gethostname(),
            ),
            GlobusEndpoint(
                uuid='EP2UUID',
                endpoint_path='/~/',
                local_path=file_dir,
                host_regex=socket.gethostname(),
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
        yield StoreInfo(GlobusStore, 'globus', {'endpoints': endpoints})

    if os.path.exists(file_dir):  # pragma: no cover
        shutil.rmtree(file_dir)


@pytest.fixture
def endpoint_store(tmp_dir: str) -> Generator[StoreInfo, None, None]:
    """Endpoint Store fixture."""
    cfg = EndpointConfig(
        name='test-endpoint',
        uuid=uuid.uuid4(),
        host='localhost',
        port=random.randint(5000, 5500),
    )
    endpoint_dir = os.path.join(tmp_dir, cfg.name)
    write_config(cfg, endpoint_dir)

    assert cfg.host is not None
    server_handle = launch_endpoint(
        cfg.name,
        cfg.uuid,
        cfg.host,
        cfg.port,
        None,
    )
    yield StoreInfo(
        EndpointStore,
        'endpoint',
        {'endpoints': [cfg.uuid], 'proxystore_dir': tmp_dir},
    )

    server_handle.terminate()
    server_handle.join()


@pytest.fixture
def ucx_store() -> Generator[StoreInfo, None, None]:
    """UCX Store fixture."""
    port = open_port()

    with mock.patch('multiprocessing.Process.start'), mock.patch(
        'multiprocessing.Process.terminate',
    ):
        yield StoreInfo(
            UCXStore,
            'ucx',
            {'interface': 'localhost', 'port': port},
        )


@pytest.fixture
def margo_store() -> Generator[StoreInfo, None, None]:
    """Margo Store fixture."""
    port = open_port()

    with mock.patch('multiprocessing.Process.start'), mock.patch(
        'multiprocessing.Process.terminate',
    ):
        yield StoreInfo(
            MargoStore,
            'margo',
            {'protocol': 'tcp', 'interface': 'localhost', 'port': port},
        )


@pytest.fixture
def websocket_store() -> Generator[StoreInfo, None, None]:
    """Websocket store fixture."""
    port = open_port()

    yield StoreInfo(
        WebsocketStore,
        'websocket',
        {'interface': '127.0.0.1', 'port': port},
    )


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
        print('help', store)
        return MargoStoreKey(
            str(uuid.uuid4()),
            0,
            f'127.0.0.1:{store.kwargs["port"]}',
        )
    elif isinstance(store, UCXStore):
        return UCXStoreKey(
            str(uuid.uuid4()),
            0,
            f'127.0.0.1:{store.kwargs["port"]}',
        )
    elif isinstance(store, WebsocketStore):
        return WebsocketStoreKey(
            str(uuid.uuid4()),
            0,
            f'ws://127.0.0.1:{store.kwargs["port"]}',
        )
    else:
        raise AssertionError(f'Unsupported store type {type(store).__name__}')

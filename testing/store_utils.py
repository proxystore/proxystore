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
from unittest import mock

import globus_sdk
import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.store.base import Store
from proxystore.store.endpoint import EndpointStore
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.store.local import LocalStore
from proxystore.store.redis import RedisStore
from testing.endpoint import launch_endpoint

FIXTURE_LIST = [
    'local_store',
    'file_store',
    'redis_store',
    'globus_store',
    'endpoint_store',
]
MOCK_REDIS_CACHE: dict[str, Any] = {}


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
        if isinstance(value, (int, float)):
            value = str(value)
        if isinstance(value, str):
            value = value.encode()
        self.data[key] = value


class StoreInfo(NamedTuple):
    """Info needed to initialize an arbitrary Store."""

    type: type[Store]
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
    if os.path.exists(file_dir):  # pragma: no branch
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

    if os.path.exists(file_dir):  # pragma: no branch
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

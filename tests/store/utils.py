"""Mocking utilities for GlobusStore tests."""
import socket
import uuid
from typing import Any
from typing import Union

import globus_sdk
import redis
from _pytest.monkeypatch import MonkeyPatch
from parsl.data_provider import globus

from proxystore.store.file import FileFactory
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusEndpoint
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusFactory
from proxystore.store.globus import GlobusStore
from proxystore.store.local import LocalFactory
from proxystore.store.local import LocalStore
from proxystore.store.redis import RedisFactory
from proxystore.store.redis import RedisStore

REDIS_HOST = "localhost"
REDIS_PORT = 59465
FILE_DIR = "/tmp/proxystore-test-298711396448"
MOCK_GLOBUS_ENDPOINTS = GlobusEndpoints(
    [
        GlobusEndpoint(
            uuid="EP1UUID",
            endpoint_path="/~/",
            local_path=FILE_DIR,
            host_regex="localhost",
        ),
        GlobusEndpoint(
            uuid="EP2UUID",
            endpoint_path="/~/",
            local_path=FILE_DIR,
            host_regex="localhost",
        ),
    ],
)
LOCAL_STORE = {
    "type": LocalStore,
    "name": "local",
    "kwargs": {},
    "factory": LocalFactory,
}
FILE_STORE = {
    "type": FileStore,
    "name": "file",
    "kwargs": {"store_dir": FILE_DIR},
    "factory": FileFactory,
}
REDIS_STORE = {
    "type": RedisStore,
    "name": "redis",
    "kwargs": {"hostname": REDIS_HOST, "port": REDIS_PORT},
    "factory": RedisFactory,
}
GLOBUS_STORE = {
    "type": GlobusStore,
    "name": "globus",
    "kwargs": {"endpoints": MOCK_GLOBUS_ENDPOINTS},
    "factory": GlobusFactory,
}


class MockTransferData:
    """Mock the Globus TransferData."""

    def __init__(self, *args, **kwargs):
        """Init MockTransferData."""
        pass

    def __setitem__(self, key, item):
        """Set item."""
        self.__dict__[key] = item

    def add_item(self, source_path: str, destination_path: str, **kwargs):
        """Add item."""
        assert isinstance(source_path, str)
        assert isinstance(destination_path, str)
        return


class MockDeleteData:
    """Mock the Globus DeleteData."""

    def __init__(self, *args, **kwargs):
        """Init MockDeleteData."""
        pass

    def __setitem__(self, key, item):
        """Set item."""
        self.__dict__[key] = item

    def add_item(self, path: str, **kwargs):
        """Add item."""
        assert isinstance(path, str)
        return


class MockTransferClient:
    """Mock the Globus TransferClient."""

    def __init__(self, *args, **kwargs):
        """Init MockTransferClient."""
        pass

    def get_task(self, task_id: str):
        """Get task."""
        assert isinstance(task_id, str)
        return None

    def submit_delete(self, delete_data: MockDeleteData):
        """Submit DeleteData."""
        assert isinstance(delete_data, MockDeleteData)
        return {"task_id": str(uuid.uuid4())}

    def submit_transfer(self, transfer_data: MockTransferData):
        """Submit TransferData."""
        assert isinstance(transfer_data, MockTransferData)
        return {"task_id": str(uuid.uuid4())}

    def task_wait(self, task_id: str, **kwargs):
        """Wait on tasks."""
        assert isinstance(task_id, str)
        return True


class MockGlobusAuth:
    """Mock Parsl GlobusAuth."""

    def __init__(self):
        """Init MockGlobusAuth."""
        self.authorizer = None


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

    def set(self, key: str, value: Union[str, bytes, int, float]) -> None:
        """Set value in MockStrictRedis."""
        if isinstance(value, (int, float)):
            value = str(value)
        if isinstance(value, str):
            value = value.encode()
        self.data[key] = value


def mock_third_party_libs() -> MonkeyPatch:
    """Get MonkeyPatch object for third party libs used by ProxyStore."""
    mpatch = MonkeyPatch()
    # Make new global MOCK_REDIS_CACHE
    global MOCK_REDIS_CACHE
    MOCK_REDIS_CACHE = {}
    mpatch.setattr(globus, "get_globus", MockGlobusAuth)
    mpatch.setattr(globus_sdk, "TransferClient", MockTransferClient)
    mpatch.setattr(globus_sdk, "DeleteData", MockDeleteData)
    mpatch.setattr(globus_sdk, "TransferData", MockTransferData)
    mpatch.setattr(socket, "gethostname", lambda: "localhost")
    mpatch.setattr(redis, "StrictRedis", MockStrictRedis)
    return mpatch

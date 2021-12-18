"""Mocking utilities for GlobusStore tests"""
import socket
import uuid

import globus_sdk
from parsl.data_provider import globus

from proxystore.store.globus import GlobusEndpoint, GlobusEndpoints
from proxystore.store.globus import GlobusStore, GlobusFactory
from proxystore.store.file import FileStore, FileFactory
from proxystore.store.local import LocalStore, LocalFactory
from proxystore.store.redis import RedisStore, RedisFactory

from _pytest.monkeypatch import MonkeyPatch

REDIS_HOST = 'localhost'
REDIS_PORT = 59465
FILE_DIR = "/tmp/proxystore-test-298711396448"
MOCK_GLOBUS_ENDPOINTS = GlobusEndpoints(
    GlobusEndpoint(
        uuid='EP1UUID',
        endpoint_path='/~/',
        local_path=FILE_DIR,
        host_regex='localhost',
    ),
    GlobusEndpoint(
        uuid='EP2UUID',
        endpoint_path='/~/',
        local_path=FILE_DIR,
        host_regex='localhost',
    ),
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


class MockTransferClient:
    def __init__(self, *args, **kwargs):
        pass

    def get_task(self, *args, **kwargs):
        return None

    def submit_delete(self, *args, **kwargs):
        return {"task_id": str(uuid.uuid4())}

    def submit_transfer(self, *args, **kwargs):
        return {"task_id": str(uuid.uuid4())}

    def task_wait(self, *args, **kwargs):
        return True


class MockTransferData:
    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def add_item(self, *args, **kwargs):
        return


class MockDeleteData:
    def __init__(self, *args, **kwargs):
        pass

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def add_item(self, *args, **kwargs):
        return


def mock_get_globus():
    """Mock get_globus()"""

    class MockGlobusAuth:
        def __init__(self):
            self.authorizer = None

    return MockGlobusAuth()


def mock_globus_and_parsl():
    """Helper function for mocking globus and parsl objects"""
    mpatch = MonkeyPatch()
    mpatch.setattr(globus, "get_globus", mock_get_globus)
    mpatch.setattr(globus_sdk, "TransferClient", MockTransferClient)
    mpatch.setattr(globus_sdk, "DeleteData", MockDeleteData)
    mpatch.setattr(globus_sdk, "TransferData", MockTransferData)
    mpatch.setattr(socket, "gethostname", lambda: "localhost")
    return mpatch

from __future__ import annotations

import os
import pathlib
from unittest import mock

import pytest

from proxystore.connectors.globus import GlobusEndpoint
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.local import LocalConnector
from proxystore.connectors.multi import Policy
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import write_config
from proxystore.store.endpoint import EndpointStore
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusStore
from proxystore.store.local import LocalStore
from proxystore.store.multi import MultiStore
from proxystore.store.redis import RedisStore
from proxystore.utils import hostname
from testing.mocked.globus import MockDeleteData
from testing.mocked.globus import MockTransferClient


def test_endpoint_store(
    endpoint: EndpointConfig,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    tmp_path = tmp_path_factory.mktemp('endpoint-store-fixture')
    tmp_dir = str(tmp_path)
    endpoint_dir = os.path.join(tmp_dir, endpoint.name)
    write_config(endpoint, endpoint_dir)

    with EndpointStore(
        'endpoint',
        endpoints=[endpoint.uuid],
        proxystore_dir=tmp_dir,
    ):
        pass


def test_file_store(tmp_path: pathlib.Path) -> None:
    with FileStore('file', store_dir=str(tmp_path / 'store-cache')):
        pass


def test_globus_store(tmp_path: pathlib.Path) -> None:
    file_dir = str(tmp_path / 'globus-store')
    endpoints = GlobusEndpoints(
        [
            GlobusEndpoint(
                uuid='EP1UUID',
                endpoint_path='/~/',
                local_path=file_dir,
                host_regex=hostname(),
            ),
            GlobusEndpoint(
                uuid='EP2UUID',
                endpoint_path='/~/',
                local_path=file_dir,
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
    ):
        with GlobusStore('globus', endpoints=endpoints):
            pass


def test_local_store() -> None:
    with LocalStore('local'):
        pass


def test_multi_store() -> None:
    with MultiStore(
        'multi',
        connectors={'local': (LocalConnector(), Policy())},
    ):
        pass


def test_redis_store() -> None:
    with mock.patch('redis.StrictRedis'):
        with RedisStore('redis', hostname='localhost', port=0):
            pass

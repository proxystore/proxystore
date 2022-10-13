"""ProxyStore endpoint commands tests."""
from __future__ import annotations

import logging
import os
from unittest import mock

from proxystore.endpoint.commands import configure_endpoint
from proxystore.endpoint.commands import list_endpoints
from proxystore.endpoint.commands import remove_endpoint
from proxystore.endpoint.commands import start_endpoint
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import read_config

_NAME = 'default'
_UUID = 'a128eee9-bcf8-44eb-b4ec-ce725b1e5167'
_PORT = 1234
_SERVER = None


def test_configure_endpoint_basic(tmp_dir, caplog) -> None:
    caplog.set_level(logging.INFO)

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        server=_SERVER,
        proxystore_dir=tmp_dir,
    )
    assert rv == 0

    endpoint_dir = os.path.join(tmp_dir, _NAME)
    assert os.path.exists(endpoint_dir)

    cfg = read_config(endpoint_dir)
    assert cfg.name == _NAME
    assert cfg.host is None
    assert cfg.port == _PORT
    assert cfg.server == _SERVER

    assert any(
        [
            str(cfg.uuid) in record.message and record.levelname == 'INFO'
            for record in caplog.records
        ],
    )


def test_configure_endpoint_home_dir(tmp_dir) -> None:
    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=tmp_dir,
    ):
        rv = configure_endpoint(
            name=_NAME,
            port=_PORT,
            server=_SERVER,
        )
    assert rv == 0

    endpoint_dir = os.path.join(tmp_dir, _NAME)
    assert os.path.exists(endpoint_dir)


def test_configure_endpoint_invalid_name(caplog) -> None:
    caplog.set_level(logging.ERROR)

    rv = configure_endpoint(
        name='abc?',
        port=_PORT,
        server=_SERVER,
    )
    assert rv == 1

    assert any(['alphanumeric' in record.message for record in caplog.records])


def test_configure_endpoint_already_exists_error(tmp_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        server=_SERVER,
        proxystore_dir=tmp_dir,
    )
    assert rv == 0

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        server=_SERVER,
        proxystore_dir=tmp_dir,
    )
    assert rv == 1

    assert any(
        ['already exists' in record.message for record in caplog.records],
    )


def test_list_endpoints(tmp_dir, caplog) -> None:
    caplog.set_level(logging.INFO)

    names = ['ep1', 'ep2', 'ep3']
    # Raise logging level while creating endpoint so we just get logs from
    # list_endpoints()
    with caplog.at_level(logging.CRITICAL):
        for name in names:
            configure_endpoint(
                name=name,
                port=_PORT,
                server=_SERVER,
                proxystore_dir=tmp_dir,
            )

    rv = list_endpoints(proxystore_dir=tmp_dir)
    assert rv == 0

    assert len(caplog.records) == len(names) + 2
    for name in names:
        assert any([name in record.message for record in caplog.records])


def test_list_endpoints_empty(tmp_dir, caplog) -> None:
    caplog.set_level(logging.INFO)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=tmp_dir,
    ):
        rv = list_endpoints()
    assert rv == 0

    assert len(caplog.records) == 1
    assert 'No valid endpoint configurations' in caplog.records[0].message


def test_remove_endpoint(tmp_dir, caplog) -> None:
    caplog.set_level(logging.INFO)

    configure_endpoint(
        name=_NAME,
        port=_PORT,
        server=_SERVER,
        proxystore_dir=tmp_dir,
    )
    assert len(get_configs(tmp_dir)) == 1

    remove_endpoint(_NAME, proxystore_dir=tmp_dir)
    assert len(get_configs(tmp_dir)) == 0

    assert any(
        ['Removed endpoint' in record.message for record in caplog.records],
    )


def test_remove_endpoints_does_not_exist(tmp_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=tmp_dir,
    ):
        rv = remove_endpoint(_NAME)
    assert rv == 1

    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_start_endpoint(tmp_dir) -> None:
    configure_endpoint(
        name=_NAME,
        port=_PORT,
        server=_SERVER,
        proxystore_dir=tmp_dir,
    )
    with mock.patch('proxystore.endpoint.commands.serve', autospec=True):
        rv = start_endpoint(_NAME, proxystore_dir=tmp_dir)
    assert rv == 0


def test_start_endpoint_does_not_exist(tmp_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=tmp_dir,
    ):
        rv = start_endpoint(_NAME)
    assert rv == 1

    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_start_endpoint_missing_config(tmp_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)

    os.makedirs(os.path.join(tmp_dir, _NAME))
    rv = start_endpoint(_NAME, proxystore_dir=tmp_dir)
    assert rv == 1

    assert any(
        [
            'does not have a config file' in record.message
            for record in caplog.records
        ],
    )


def test_start_endpoint_bad_config(tmp_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)

    endpoint_dir = os.path.join(tmp_dir, _NAME)
    os.makedirs(endpoint_dir)
    with open(os.path.join(endpoint_dir, 'endpoint.json'), 'w') as f:
        f.write('not valid json')

    rv = start_endpoint(_NAME, proxystore_dir=tmp_dir)
    assert rv == 1

    assert any(
        ['Unable to parse' in record.message for record in caplog.records],
    )

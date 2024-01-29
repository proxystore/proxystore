from __future__ import annotations

import os
import pathlib
import uuid
from typing import Any

import pytest

from proxystore.endpoint.config import ENDPOINT_CONFIG_FILE
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointRelayConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import get_log_filepath
from proxystore.endpoint.config import get_pid_filepath
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import validate_name
from proxystore.endpoint.config import write_config


def test_write_read_config(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'config-dir')
    assert not os.path.exists(tmp_dir)

    cfg = EndpointConfig(
        name='name',
        uuid=str(uuid.uuid4()),
        host='host',
        port=1234,
    )
    write_config(cfg, tmp_dir)
    assert os.path.exists(tmp_dir)

    # Overwriting is okay
    write_config(cfg, tmp_dir)

    new_cfg = read_config(tmp_dir)
    assert cfg == new_cfg


def test_read_config_missing_file(tmp_path: pathlib.Path) -> None:
    os.makedirs(tmp_path, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        read_config(str(tmp_path))


def test_get_configs(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'config-dir')
    assert not os.path.exists(tmp_dir)
    # dir does not exists so empty list should be returned
    assert len(get_configs(tmp_dir)) == 0

    os.makedirs(tmp_dir, exist_ok=True)
    assert len(get_configs(tmp_dir)) == 0

    names = ['ep1', 'ep2', 'ep3']
    for name in names:
        endpoint_dir = os.path.join(tmp_dir, name)
        write_config(
            EndpointConfig(
                name=name,
                uuid=str(uuid.uuid4()),
                host='host',
                port=1234,
            ),
            endpoint_dir,
        )

    # Make invalid directory to make sure get_configs skips it
    os.makedirs(os.path.join(tmp_dir, 'ep4'))
    # Make a bad config to make sure its skipped
    ep5 = os.path.join(tmp_dir, 'ep5')
    os.makedirs(ep5)
    with open(os.path.join(ep5, ENDPOINT_CONFIG_FILE), 'w') as f:
        f.write('this is not json')
    # Make another bad config to make sure its skipped
    ep6 = os.path.join(tmp_dir, 'ep6')
    os.makedirs(ep6)
    with open(os.path.join(ep6, ENDPOINT_CONFIG_FILE), 'w') as f:
        f.write('{"name": "this is missing keys"}')

    configs = get_configs(tmp_dir)
    assert len(configs) == len(names)
    found_names = {cfg.name for cfg in configs}
    assert set(names) == found_names


@pytest.mark.parametrize(
    ('name', 'valid'),
    (
        ('abc', True),
        ('ABC', True),
        ('aBc_', True),
        ('aBc-', True),
        ('aBc_-123', True),
        ('', False),
        ('abc.', False),
        ('abc?', False),
        ('abc/', False),
        ('abc~', False),
    ),
)
def test_validate_name(name: str, valid: bool) -> None:
    assert validate_name(name) == valid


@pytest.mark.parametrize(
    ('bad_cfg', 'valid'),
    (
        ({}, True),
        ({'name': 'bad name'}, False),
        ({'uuid': 'abc-abc-abc'}, False),
        ({'port': 0}, False),
        ({'port': 1000000}, False),
    ),
)
def test_validate_config(bad_cfg: Any, valid: bool) -> None:
    options = {
        'name': 'name',
        'uuid': str(uuid.uuid4()),
        'host': 'host',
        'port': 1234,
    }
    options.update(bad_cfg)

    if valid:
        EndpointConfig(**options)
    else:
        with pytest.raises(ValueError):
            EndpointConfig(**options)


@pytest.mark.parametrize(
    ('bad_cfg', 'valid'),
    (
        ({'address': 'ws://'}, True),
        ({'address': 'wss://'}, True),
        ({'address': ''}, False),
        ({'address': 'https://'}, False),
        ({'peer_channels': 1}, True),
        ({'peer_channels': 0}, False),
    ),
)
def test_validate_relay_config(bad_cfg: Any, valid: bool) -> None:
    if valid:
        EndpointRelayConfig(**bad_cfg)
    else:
        with pytest.raises(ValueError):
            EndpointRelayConfig(**bad_cfg)


@pytest.mark.parametrize(
    ('bad_cfg', 'valid'),
    (
        ({'max_object_size': 0}, False),
        ({'max_object_size': 1}, True),
        ({'max_object_size': -1}, False),
    ),
)
def test_validate_storage_config(bad_cfg: Any, valid: bool) -> None:
    if valid:
        EndpointStorageConfig(**bad_cfg)
    else:
        with pytest.raises(ValueError):
            EndpointStorageConfig(**bad_cfg)


def test_get_pid_filepath() -> None:
    fp = get_pid_filepath('/tmp')
    assert isinstance(fp, str)
    assert not os.path.exists(fp)


def test_get_log_filepath() -> None:
    fp = get_log_filepath('/tmp')
    assert isinstance(fp, str)
    assert not os.path.exists(fp)

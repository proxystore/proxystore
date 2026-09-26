from __future__ import annotations

import os
import pathlib
import stat
import uuid
from typing import Any

import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointRelayConfig
from proxystore.endpoint.config import EndpointRelayICEServerConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.config import validate_name
from proxystore.endpoint.directory import EndpointDir


def test_write_read_config(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'config-dir')
    assert not os.path.exists(tmp_dir)

    cfg = EndpointConfig(
        name='name',
        uuid=str(uuid.uuid4()),
        host='host',
        port=1234,
    )
    EndpointDir(tmp_dir).write_config(cfg)
    assert os.path.exists(tmp_dir)
    assert stat.S_IMODE(os.stat(tmp_dir).st_mode) == 0o700

    # Overwriting is okay
    EndpointDir(tmp_dir).write_config(cfg)

    new_cfg = EndpointDir(tmp_dir).read_config()
    assert cfg == new_cfg


def test_write_read_config_with_ice_servers(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'config-dir')

    cfg = EndpointConfig(
        name='name',
        uuid=str(uuid.uuid4()),
        host='host',
        port=1234,
    )
    cfg.relay.ice_servers = [
        EndpointRelayICEServerConfig(urls='stun:stun.example.com:3478'),
        EndpointRelayICEServerConfig(
            urls=['turn:turn.example.com:3478'],
            username='user',
            credential='secret',
        ),
    ]
    EndpointDir(tmp_dir).write_config(cfg)

    new_cfg = EndpointDir(tmp_dir).read_config()
    assert cfg == new_cfg


def test_read_config_missing_file(tmp_path: pathlib.Path) -> None:
    os.makedirs(tmp_path, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        EndpointDir(str(tmp_path)).read_config()


def test_get_configs(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'config-dir')
    assert not os.path.exists(tmp_dir)
    # dir does not exists so empty list should be returned
    assert len([c for _, c in EndpointDir.find_all(tmp_dir)]) == 0

    os.makedirs(tmp_dir, exist_ok=True)
    assert len([c for _, c in EndpointDir.find_all(tmp_dir)]) == 0

    names = ['ep1', 'ep2', 'ep3']
    for name in names:
        endpoint_dir = EndpointDir(os.path.join(tmp_dir, name))
        endpoint_dir.write_config(
            EndpointConfig(
                name=name,
                uuid=str(uuid.uuid4()),
                host='host',
                port=1234,
            )
        )

    # Make invalid directory to make sure get_configs skips it
    os.makedirs(os.path.join(tmp_dir, 'ep4'))
    # Make a bad config to make sure its skipped
    ep5 = os.path.join(tmp_dir, 'ep5')
    os.makedirs(ep5)
    with open(EndpointDir(ep5).config_path, 'w') as f:
        f.write('this is not json')
    # Make another bad config to make sure its skipped
    ep6 = os.path.join(tmp_dir, 'ep6')
    os.makedirs(ep6)
    with open(EndpointDir(ep6).config_path, 'w') as f:
        f.write('{"name": "this is missing keys"}')

    configs = [c for _, c in EndpointDir.find_all(tmp_dir)]
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

from __future__ import annotations

import os
import pathlib
import stat
import uuid
from typing import Any

import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.config import validate_name
from proxystore.endpoint.directory import EndpointDir
from testing.endpoint import random_endpoint_id


def test_write_read_config(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'config-dir')
    assert not os.path.exists(tmp_dir)

    cfg = EndpointConfig(
        name='name',
        id=random_endpoint_id(),
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
                id=random_endpoint_id(),
                host='host',
                port=1234,
            )
        )

    # Make invalid directory to make sure find_all skips it
    os.makedirs(os.path.join(tmp_dir, 'ep4'))
    # Nested directories and files are not endpoints
    EndpointDir(os.path.join(tmp_dir, 'ep1', 'nested')).write_config(
        EndpointConfig(name='nested', id=random_endpoint_id(), port=1234),
    )
    with open(os.path.join(tmp_dir, 'file'), 'w') as f:
        f.write('not an endpoint')
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
    ('bad_cfg', 'error'),
    (
        ({}, None),
        ({'name': 'bad name'}, 'alphanumeric characters'),
        ({'id': 'abc-abc-abc'}, 'not a valid endpoint ID'),
        ({'id': 42}, 'not a valid endpoint ID'),
        ({'port': 0}, 'Port must be in range'),
        ({'port': 1000000}, 'Port must be in range'),
    ),
)
def test_validate_config(bad_cfg: Any, error: str | None) -> None:
    options = {
        'name': 'name',
        'id': random_endpoint_id(),
        'host': 'host',
        'port': 1234,
    }
    options.update(bad_cfg)

    if error is None:
        EndpointConfig(**options)
    else:
        with pytest.raises(ValueError, match=error):
            EndpointConfig(**options)


@pytest.mark.parametrize(
    ('bad_cfg', 'error'),
    (
        ({'max_object_size': 0}, None),
        ({'max_object_size': 1}, None),
        ({'max_object_size': -1}, 'zero \\(no limit\\) or greater'),
    ),
)
def test_validate_storage_config(bad_cfg: Any, error: str | None) -> None:
    if error is None:
        EndpointStorageConfig(**bad_cfg)
    else:
        with pytest.raises(ValueError, match=error):
            EndpointStorageConfig(**bad_cfg)


def test_storage_config_object_size_limit() -> None:
    assert EndpointStorageConfig().object_size_limit == (
        EndpointStorageConfig().max_object_size
    )
    assert EndpointStorageConfig(max_object_size=10).object_size_limit == 10
    assert EndpointStorageConfig(max_object_size=0).object_size_limit is None


def test_legacy_uuid_config(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match='older version of ProxyStore'):
        EndpointConfig.model_validate(
            {'name': 'name', 'uuid': str(uuid.uuid4()), 'port': 1234},
        )

    endpoint_dir = EndpointDir(str(tmp_path))
    with open(endpoint_dir.config_path, 'w') as f:
        f.write(f'name = "name"\nuuid = "{uuid.uuid4()}"\nport = 1234\n')
    with pytest.raises(ValueError, match='configure it again'):
        endpoint_dir.read_config()

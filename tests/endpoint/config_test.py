from __future__ import annotations

import os
import pathlib
import stat
import uuid
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.config import check_name
from proxystore.endpoint.config import CONFIG_VERSION
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.config import resolve_host
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.identity import EndpointId


def test_write_read_config(tmp_path: pathlib.Path) -> None:
    tmp_dir = os.path.join(tmp_path, 'my-ep')
    assert not os.path.exists(tmp_dir)

    cfg = EndpointConfig(
        name='my-ep',
        id=EndpointId.random(),
        host='host',
        port=1234,
        p2p=EndpointP2PConfig(relays=['https://relay.example.com']),
    )
    EndpointDir(tmp_dir).write_config(cfg)
    assert os.path.exists(tmp_dir)
    assert stat.S_IMODE(os.stat(tmp_dir).st_mode) == 0o700

    # Overwriting is okay
    EndpointDir(tmp_dir).write_config(cfg)

    new_cfg = EndpointDir(tmp_dir).read_config()
    assert cfg == new_cfg


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
                id=EndpointId.random(),
                host='host',
                port=1234,
            )
        )

    # Make invalid directory to make sure find_all skips it
    os.makedirs(os.path.join(tmp_dir, 'ep4'))
    # Nested directories and files are not endpoints
    EndpointDir(os.path.join(tmp_dir, 'ep1', 'nested')).write_config(
        EndpointConfig(name='nested', id=EndpointId.random(), port=1234),
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
def test_check_name(name: str, valid: bool) -> None:
    if valid:
        assert check_name(name, 'Test') == name
    else:
        with pytest.raises(ValueError, match='Test names must only contain'):
            check_name(name, 'Test')


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
        'id': EndpointId.random(),
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
    ('options', 'error'),
    (
        ({}, None),
        ({'backend': 'sqlite'}, None),
        ({'backend': 'sqlite', 'database_path': '/tmp/db'}, None),
        ({'database_path': 'blobs.db'}, 'only used by the "sqlite" backend'),
        ({'backend': 'redis'}, 'memory'),
    ),
)
def test_validate_storage_config(options: Any, error: str | None) -> None:
    if error is None:
        EndpointStorageConfig(**options)
    else:
        with pytest.raises(ValueError, match=error):
            EndpointStorageConfig(**options)


def test_object_size_limit() -> None:
    def _config(**kwargs: Any) -> EndpointConfig:
        return EndpointConfig(
            name='name',
            id=EndpointId.random(),
            port=1234,
            **kwargs,
        )

    assert _config().object_size_limit == _config().max_object_size
    assert _config(max_object_size=10).object_size_limit == 10
    assert _config(max_object_size=0).object_size_limit is None
    with pytest.raises(ValueError, match=r'zero \(no limit\) or greater'):
        _config(max_object_size=-1)


@pytest.mark.parametrize(
    ('value', 'expected'),
    (('100 MB', 100_000_000), ('1GiB', 2**30), ('1000', 1000), ('0', 0)),
)
def test_max_object_size_with_units(value: str, expected: int) -> None:
    config = EndpointConfig(
        name='name',
        id=EndpointId.random(),
        port=1234,
        max_object_size=value,
    )
    assert config.max_object_size == expected


def test_max_object_size_with_units_invalid() -> None:
    with pytest.raises(ValueError, match='Unknown unit'):
        EndpointConfig(
            name='name',
            id=EndpointId.random(),
            port=1234,
            max_object_size='100 XB',
        )
    with pytest.raises(ValueError, match=r'zero \(no limit\) or greater'):
        EndpointConfig(
            name='name',
            id=EndpointId.random(),
            port=1234,
            max_object_size='-1 MB',
        )


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


@pytest.mark.parametrize(
    ('relays', 'error'),
    (
        ('n0', None),
        ('none', None),
        (['https://relay.example.com', 'http://localhost:3340'], None),
        ('other', 'Input should be'),
        ([], 'at least one URL'),
        (['relay.example.com'], 'must start with http'),
    ),
)
def test_validate_p2p_relays(relays: Any, error: str | None) -> None:
    if error is None:
        assert EndpointP2PConfig(relays=relays).relays == relays
    else:
        with pytest.raises(ValueError, match=error):
            EndpointP2PConfig(relays=relays)


def test_read_config_name_mismatch(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('my-ep', str(tmp_path), port=1234)
    config = endpoint_dir.read_config()
    endpoint_dir.write_config(config.model_copy(update={'name': 'other'}))
    with pytest.raises(ValueError, match='does not match the name of the'):
        endpoint_dir.read_config()
    # Endpoints with an invalid configuration are not found
    assert EndpointDir.find_all(str(tmp_path)) == []


def _options(**kwargs: Any) -> dict[str, Any]:
    return {'name': 'name', 'id': EndpointId.random(), 'port': 1234, **kwargs}


def test_config_version() -> None:
    assert EndpointConfig(**_options()).version == CONFIG_VERSION
    with pytest.raises(ValueError, match='only supports version 1'):
        EndpointConfig(**_options(version=2))


@pytest.mark.parametrize(
    'extra',
    ({'host_type': 'ip'}, {'storage': {'database': 'x'}}),
)
def test_config_unknown_fields(extra: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match='Extra inputs are not permitted'):
        EndpointConfig(**_options(**extra))


def test_config_host() -> None:
    assert EndpointConfig(**_options()).host == 'ip'
    assert EndpointConfig(**_options(host=' 10.0.0.1 ')).host == '10.0.0.1'
    assert EndpointConfig(**_options(host=' IP ')).host == 'ip'
    assert EndpointConfig(**_options(host='FQDN')).host == 'fqdn'
    with pytest.raises(ValueError, match='Host must be'):
        EndpointConfig(**_options(host=' '))


def test_resolve_host() -> None:
    with (
        mock.patch('socket.gethostbyname', return_value='10.0.0.1'),
        mock.patch('socket.getfqdn', return_value='node.example.com'),
    ):
        assert resolve_host('ip') == '10.0.0.1'
        assert resolve_host('fqdn') == 'node.example.com'
        assert resolve_host('10.0.0.2') == '10.0.0.2'

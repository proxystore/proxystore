"""Tests for Endpoint config utilities."""
from __future__ import annotations

import os

import pytest

from proxystore.endpoint.config import default_dir
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config


def test_default_dir() -> None:
    assert isinstance(default_dir(), str)
    assert os.path.isabs(default_dir())


def test_write_read_config(tmp_dir) -> None:
    assert not os.path.exists(tmp_dir)

    cfg = EndpointConfig(
        name='name',
        uuid='uuid',
        host='host',
        port=1234,
        server=None,
    )
    write_config(cfg, tmp_dir)
    assert os.path.exists(tmp_dir)

    # Overwriting is okay
    write_config(cfg, tmp_dir)

    new_cfg = read_config(tmp_dir)
    assert cfg == new_cfg


def test_read_config_missing_file(tmp_dir) -> None:
    os.makedirs(tmp_dir, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        read_config(tmp_dir)


def test_get_configs(tmp_dir) -> None:
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
                uuid='uuid',
                host='host',
                port=1234,
            ),
            endpoint_dir,
        )

    # Make invalid directory to make sure get_configs skips it
    os.makedirs(os.path.join(tmp_dir, 'ep4'))

    configs = get_configs(tmp_dir)
    assert len(configs) == len(names)
    found_names = {cfg.name for cfg in configs}
    assert set(names) == found_names

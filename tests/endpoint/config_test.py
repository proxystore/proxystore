"""Tests for Endpoint config utilities."""
from __future__ import annotations

import os
import uuid

import pytest

from proxystore.endpoint.config import default_dir
from proxystore.endpoint.config import get_config
from proxystore.endpoint.config import save_config
from proxystore.endpoint.config import update_config


def test_default_dir() -> None:
    assert isinstance(default_dir(), str)
    assert os.path.isabs(default_dir())


def test_create_save_config(tmp_dir) -> None:
    assert not os.path.exists(tmp_dir)

    # Get config should not write anything
    cfg = get_config(tmp_dir)
    assert not os.path.exists(tmp_dir)

    name = 'my-endpoint'
    uuid_ = str(uuid.uuid4())

    cfg.name = name
    cfg.uuid = uuid_

    save_config(cfg, tmp_dir)
    assert os.path.exists(tmp_dir)

    cfg = get_config(tmp_dir)
    assert cfg.name == name
    assert cfg.uuid == uuid_


def test_update_config(tmp_dir) -> None:
    assert not os.path.exists(tmp_dir)

    name = 'my-endpoint'
    uuid_ = str(uuid.uuid4())
    host = 'localhost'
    port = 1234

    # Update config should create new config is none exists
    update_config(tmp_dir, name=name, uuid=uuid_)
    cfg = get_config(tmp_dir)
    assert cfg.name == name
    assert cfg.uuid == uuid_
    assert cfg.host is None
    assert cfg.port is None

    update_config(tmp_dir, host=host, port=port)
    cfg = get_config(tmp_dir)
    assert cfg.name == name
    assert cfg.uuid == uuid_
    assert cfg.host == host
    assert cfg.port == port


def test_bad_update_config(tmp_dir) -> None:
    with pytest.raises(AttributeError, match='attribute'):
        update_config(tmp_dir, fake_key='abc')

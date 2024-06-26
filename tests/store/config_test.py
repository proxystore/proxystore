from __future__ import annotations

import pathlib

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.store.base import Store
from proxystore.store.config import ConnectorConfig
from proxystore.store.config import StoreConfig


@pytest.mark.parametrize(
    ('kind', 'expected'),
    (
        ('local', LocalConnector),
        ('LOCAL', LocalConnector),
        ('LocalConnector', LocalConnector),
        ('proxystore.connectors.local.LocalConnector', LocalConnector),
    ),
)
def test_get_connector_type(kind: str, expected: type) -> None:
    config = ConnectorConfig(kind=kind)
    assert config.get_connector_type() == expected


def test_local_connector_config() -> None:
    config = ConnectorConfig(kind='local')

    connector = config.get_connector()
    assert isinstance(connector, LocalConnector)
    connector.close()


def test_file_connector_config(tmp_path: pathlib.Path) -> None:
    config = ConnectorConfig(kind='file', options={'store_dir': str(tmp_path)})

    connector = config.get_connector()
    assert isinstance(connector, FileConnector)
    connector.close()


def test_connector_config_bad_kind() -> None:
    config = ConnectorConfig(kind='fake')

    with pytest.raises(ValueError, match='fake'):
        config.get_connector()


def test_connector_config_bad_extras() -> None:
    config = ConnectorConfig(kind='local', options={'wrong_arg': True})
    assert config.options['wrong_arg']

    with pytest.raises(TypeError, match='wrong_arg'):
        config.get_connector()


def test_to_from_config() -> None:
    original = Store('test-to-from-config', LocalConnector(), register=False)
    original_config = original.config()

    new = Store.from_config(original_config)
    assert new.name == original.name

    original.close()
    new.close()


def test_store_config_from_toml(tmp_path: pathlib.Path) -> None:
    config_file = tmp_path / 'config.toml'
    store_dir = tmp_path / 'cache'

    with open(config_file, 'w') as f:
        f.write(f"""\
name = "test"
cache_size = 0
metrics = true
populate_target = false

[connector]
kind = "file"
options = {{ store_dir = "{store_dir}" }}
""")

    config = StoreConfig.from_toml(config_file)
    assert config.name == 'test'
    assert config.cache_size == 0
    assert config.metrics
    assert not config.populate_target

    assert config.connector.kind == 'file'
    assert config.connector.options['store_dir'] == str(store_dir)


def test_store_config_write_toml(tmp_path: pathlib.Path) -> None:
    config_file = tmp_path / 'config.toml'
    store_dir = tmp_path / 'cache'

    config = StoreConfig(
        name='test',
        connector=ConnectorConfig(
            kind='file',
            options={'store_dir': str(store_dir)},
        ),
        cache_size=0,
        metrics=True,
        populate_target=False,
    )

    config.write_toml(config_file)
    assert config_file.is_file()

    new_config = StoreConfig.from_toml(config_file)
    assert config == new_config

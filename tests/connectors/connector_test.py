from __future__ import annotations

from typing import Any

import proxystore.store
from proxystore.connectors.protocols import Connector
from proxystore.connectors.protocols import DeferrableConnector
from proxystore.store.base import Store


def test_connector_repr(connectors: Connector[Any]) -> None:
    assert isinstance(repr(connectors), str)


def test_connector_basic_ops(connectors: Connector[Any]) -> None:
    connector = connectors
    value = b'test_value'

    key = connector.put(value)
    assert connector.get(key) == value
    assert connector.exists(key)
    connector.evict(key)
    assert not connector.exists(key)
    assert connector.get(key) is None
    # Evicting missing key should not raise an error
    connector.evict(key)


def test_connector_batch_ops(connectors: Connector[Any]) -> None:
    connector = connectors
    values = [b'value1', b'value2', b'value3']

    keys = connector.put_batch(values)
    assert connector.get_batch(keys) == values
    assert all(connector.exists(key) for key in keys)
    for key in keys:
        connector.evict(key)
    assert all(not connector.exists(key) for key in keys)
    for key in keys:
        assert connector.get(key) is None


def test_connector_config(connectors: Connector[Any]) -> None:
    # This tests also tests multiple connectors being initialized at the
    # same time.
    connector = connectors

    config = connector.config()
    new_connector = type(connector).from_config(config)

    assert isinstance(new_connector, Connector)
    assert type(connector) is type(new_connector)


def test_connector_store_config(connectors: Connector[Any]) -> None:
    # The store recreates the connector from the store config when a proxy
    # is resolved in another process.
    store = Store(connectors)
    new_store = Store.from_config(store.config())
    assert type(new_store.connector) is type(connectors)

    # Unregister rather than close the stores because closing would close
    # the connector fixture which is shared by other tests.
    proxystore.store._unregister_store(store)
    proxystore.store._unregister_store(new_store)


def test_deferrable_connector_ops(connectors: Connector[Any]) -> None:
    connector = connectors

    if isinstance(connector, DeferrableConnector):
        obj = b'test_value'
        key = connector.new_key(obj)
        assert not connector.exists(key)
        connector.set(key, obj)
        connector.set(key, obj)
        assert connector.get(key) == obj


def test_deferrable_connector_is_connector() -> None:
    assert issubclass(DeferrableConnector, Connector)

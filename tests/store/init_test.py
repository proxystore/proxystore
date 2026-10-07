from __future__ import annotations

import pickle

import pytest

import proxystore.store
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import get_or_create_store
from proxystore.store import get_store
from proxystore.store.base import Store
from proxystore.store.exceptions import NonProxiableTypeError
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.exceptions import StoreError
from testing.factories import SimpleFactory


def _is_registered(store: Store[LocalConnector]) -> bool:
    return proxystore.store._registry._stores.get(store.id) is store


def test_store_registered_until_closed() -> None:
    store = Store(LocalConnector())
    assert _is_registered(store)
    store.close()
    assert not _is_registered(store)
    # Closing again is a no-op
    store.close()


def test_store_ids_are_unique() -> None:
    with (
        Store(LocalConnector(), name='test') as store1,
        Store(LocalConnector(), name='test') as store2,
    ):
        assert store1.id != store2.id
        assert _is_registered(store1)
        assert _is_registered(store2)


def test_get_or_create_store_existing() -> None:
    with Store(LocalConnector()) as store:
        assert get_or_create_store(store.config()) is store


def test_get_or_create_store_new() -> None:
    store = Store(LocalConnector())
    config = store.config()
    store.close()

    new_store = get_or_create_store(config)
    assert new_store is not store
    assert new_store.id == store.id
    assert _is_registered(new_store)
    assert get_or_create_store(config) is new_store
    new_store.close()


def test_get_or_create_store_without_id() -> None:
    with Store(LocalConnector()) as store:
        config = store.config().model_copy(update={'id': None})
        new_store = get_or_create_store(config)
        assert new_store is not store
        assert new_store.id != store.id
        new_store.close()


def test_from_config_does_not_replace_registered_store() -> None:
    with Store(LocalConnector()) as store:
        other = Store.from_config(store.config())
        assert other.id == store.id
        assert _is_registered(store)
        assert not _is_registered(other)

        # Closing the unregistered store does not unregister the other.
        other.close()
        assert _is_registered(store)


def test_lookup_by_proxy() -> None:
    with (
        Store(LocalConnector()) as local1,
        Store(LocalConnector()) as local2,
    ):
        local1_proxy: Proxy[list[int]] = local1.proxy([1, 2, 3])
        local2_proxy: Proxy[list[int]] = local2.proxy([1, 2, 3])

        assert get_store(local1_proxy) is local1
        assert get_store(local2_proxy) is local2

        # Make a proxy without an associated store
        f = SimpleFactory([1, 2, 3])
        p = Proxy(f)
        with pytest.raises(ProxyStoreFactoryError):
            get_store(p)


def test_same_name_proxies_resolve_with_correct_store() -> None:
    # Stores with the same name used to collide in the registry so a proxy
    # could be resolved using the wrong store.
    with (
        Store(LocalConnector(), name='default') as store1,
        Store(LocalConnector(), name='default') as store2,
    ):
        proxy1: Proxy[str] = pickle.loads(
            pickle.dumps(store1.proxy('value1', populate_target=False)),
        )
        proxy2: Proxy[str] = pickle.loads(
            pickle.dumps(store2.proxy('value2', populate_target=False)),
        )

        assert proxy1 == 'value1'
        assert proxy2 == 'value2'
        assert get_store(proxy1) is store1
        assert get_store(proxy2) is store2


def test_store_exceptions_are_store_errors() -> None:
    assert issubclass(ProxyResolveMissingKeyError, StoreError)
    assert issubclass(NonProxiableTypeError, StoreError)

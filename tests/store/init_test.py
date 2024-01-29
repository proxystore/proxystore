from __future__ import annotations

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import store_registration
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.exceptions import StoreExistsError


def test_register_unregister_store() -> None:
    store = Store('test', connector=LocalConnector())

    register_store(store)
    assert get_store('test') == store

    with pytest.raises(StoreExistsError):
        register_store(store)
    register_store(store, exist_ok=True)

    unregister_store(store.name)
    assert get_store('test') is None

    # does not raise error
    unregister_store('not a valid store name')


def test_unregister_with_store() -> None:
    store = Store('test', connector=LocalConnector())

    register_store(store)
    assert get_store('test') == store
    unregister_store(store)
    assert get_store('test') is None


def test_lookup_by_proxy() -> None:
    local1 = Store('local1', connector=LocalConnector())
    local2 = Store('local2', connector=LocalConnector())

    with store_registration(local1, local2):
        # Make a proxy with both
        local1_proxy: Proxy[list[int]] = local1.proxy([1, 2, 3])
        local2_proxy: Proxy[list[int]] = local2.proxy([1, 2, 3])

        # Make sure both look up correctly
        sl1 = get_store(local1_proxy)
        assert sl1 is not None
        assert sl1.name == local1.name
        sl2 = get_store(local2_proxy)
        assert sl2 is not None
        assert sl2.name == local2.name

        # Make a proxy without an associated store
        f = SimpleFactory([1, 2, 3])
        p = Proxy(f)
        with pytest.raises(ProxyStoreFactoryError):
            get_store(p)


def test_store_registration() -> None:
    local1 = Store('local1', connector=LocalConnector())
    local2 = Store('local2', connector=LocalConnector())

    assert get_store(local1.name) is None
    assert get_store(local2.name) is None

    with store_registration(local1, local2):
        assert get_store(local1.name) is local1
        assert get_store(local2.name) is local2

        with pytest.raises(StoreExistsError):
            with store_registration(local1):
                pass  # pragma: no cover

        with store_registration(local1, exist_ok=True):
            pass

    assert get_store(local1.name) is None
    assert get_store(local2.name) is None

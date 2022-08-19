"""Unit tests for proxystore.store.utils."""
from __future__ import annotations

import pytest

from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.local import LocalStore
from proxystore.store.utils import get_key


def test_get_key_from_proxy() -> None:
    store = LocalStore('store')

    key = store.set('value')
    proxy: Proxy[str] = store.proxy_from_key(key)

    assert get_key(proxy) == key


def test_get_key_from_proxy_not_created_by_store() -> None:
    p = Proxy(SimpleFactory('value'))

    with pytest.raises(ProxyStoreFactoryError):
        get_key(p)

"""Unit tests for proxystore.store.utils."""
from __future__ import annotations

import pytest

from proxystore.factory import SimpleFactory
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.local import LocalStore
from proxystore.store.utils import get_key
from proxystore.store.utils import resolve_async


def test_get_key_from_proxy() -> None:
    with LocalStore('store') as store:
        key = store.set('value')
        proxy: Proxy[str] = store.proxy_from_key(key)

        assert get_key(proxy) == key


def test_get_key_from_proxy_not_created_by_store() -> None:
    p = Proxy(SimpleFactory('value'))

    with pytest.raises(ProxyStoreFactoryError):
        get_key(p)


def test_async_resolve() -> None:
    with LocalStore('store') as store:
        value = 'value'
        p = store.proxy(value)

        assert not is_resolved(p)

        resolve_async(p)
        assert p == value

        assert is_resolved(p)

        # Now async resolve should be a no-op
        resolve_async(p)
        assert p == value

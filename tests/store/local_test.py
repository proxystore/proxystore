"""LocalStore Unit Tests."""
from __future__ import annotations

from pytest import raises

import proxystore as ps
from proxystore.store.exceptions import ProxyResolveMissingKey


def test_local_store_proxy() -> None:
    """Test LocalStore Proxying."""
    store = ps.store.init_store(ps.store.STORES.LOCAL, 'local')

    p = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    assert p == [1, 2, 3]
    assert store.get(ps.proxy.get_key(p)) == [1, 2, 3]

    p2 = store.proxy(key=ps.proxy.get_key(p))
    assert p2 == [1, 2, 3]

    store.proxy([2, 3, 4], key='key')
    assert store.get(key='key') == [2, 3, 4]

    # At least one of key or object must be passed
    with raises(ValueError):
        store.proxy()
    with raises(ValueError):
        store.proxy_batch()

    batch_values = ['test_value1', 'test_value2', 'test_value3']

    proxies = store.proxy_batch(batch_values)
    for p, v in zip(proxies, batch_values):
        assert p == v

    proxies = store.proxy_batch(keys=[ps.proxy.get_key(p) for p in proxies])
    for p, v in zip(proxies, batch_values):
        assert p == v

    store.close()


def test_raises_missing_key() -> None:
    """Test Proxy raises missing key error."""
    store = ps.store.init_store(ps.store.STORES.LOCAL, 'local')

    key = 'test_key'
    assert not store.exists(key)

    proxy = store.proxy(key=key)
    with raises(ProxyResolveMissingKey):
        proxy()

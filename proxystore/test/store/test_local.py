"""LocalStore Unit Tests."""
from pytest import raises

import proxystore as ps
from proxystore.store.local import LocalFactory
from proxystore.store.local import LocalStore


def test_kwargs() -> None:
    """Test LocalFactory kwargs."""
    store = LocalStore(name="local")
    assert store.kwargs == {}


def test_local_factory() -> None:
    """Test LocalFactory."""
    key = "key"
    f = LocalFactory(key, name="local")
    # Force delete LocalStore backend if it exists so resolving factory
    # raises not initialized error
    ps.store._stores = {}
    with raises(RuntimeError):
        f()

    store = ps.store.init_store(LocalStore, "local")

    key = store.set([1, 2, 3], key=key)
    f = LocalFactory(key, name="local")
    assert f() == [1, 2, 3]

    f2 = LocalFactory(key, name="local", evict=True)
    assert store.exists(key)
    assert f2() == [1, 2, 3]
    assert not store.exists(key)

    store.set([1, 2, 3], key=key)
    f = LocalFactory(key, name="local")
    f.resolve_async()
    assert f() == [1, 2, 3]

    f_str = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_str)
    assert f() == [1, 2, 3]


def test_local_store_proxy() -> None:
    """Test LocalStore Proxying."""
    store = ps.store.init_store(ps.store.STORES.LOCAL, "local")

    p = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    assert p == [1, 2, 3]
    assert store.get(ps.proxy.get_key(p)) == [1, 2, 3]

    p2 = store.proxy(key=ps.proxy.get_key(p))
    assert p2 == [1, 2, 3]

    store.proxy([2, 3, 4], key="key")
    assert store.get(key="key") == [2, 3, 4]

    # At least one of key or object must be passed
    with raises(ValueError):
        store.proxy()
    with raises(ValueError):
        store.proxy_batch()

    with raises(ValueError):
        # Cannot make proxy from key that does not exist
        store.proxy(key="missing_key")

    batch_values = ["test_value1", "test_value2", "test_value3"]

    proxies = store.proxy_batch(batch_values)
    for p, v in zip(proxies, batch_values):
        assert p == v

    proxies = store.proxy_batch(keys=[ps.proxy.get_key(p) for p in proxies])
    for p, v in zip(proxies, batch_values):
        assert p == v

    store.cleanup()

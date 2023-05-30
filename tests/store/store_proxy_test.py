"""Store Factory and Proxy Tests for Store Subclasses."""
from __future__ import annotations

from typing import Any

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.base import StoreFactory
from proxystore.store.exceptions import NonProxiableTypeError
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.utils import get_key


def test_store_factory() -> None:
    """Test Store Factory."""
    with Store('test', LocalConnector()) as store:
        key = store.put([1, 2, 3])

        # Ensure store is not registered to test if factory can reinitialize it
        assert get_store(store.name) is None

        f: StoreFactory[Any, list[int]] = StoreFactory(
            key,
            store_config=store.config(),
        )
        assert f() == [1, 2, 3]

        f2: StoreFactory[Any, list[int]] = StoreFactory(
            key,
            store_config=store.config(),
            evict=True,
        )
        assert store.exists(key)
        assert f2() == [1, 2, 3]
        assert not store.exists(key)

        key = store.put([1, 2, 3])
        # Clear store to see if factory can reinitialize it
        unregister_store(store)

        f = StoreFactory(
            key,
            store_config=store.config(),
        )
        f.resolve_async()
        assert f._obj_future is not None
        assert f() == [1, 2, 3]
        assert f._obj_future is None

        # Check factory serialization
        f_str = serialize(f)
        f = deserialize(f_str)
        assert f() == [1, 2, 3]

        unregister_store(store)


def test_factory_serialization(store: Store[LocalConnector]) -> None:
    key = store.put([1, 2, 3])
    f1: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
    )
    f1_bytes = serialize(f1)
    f2 = deserialize(f1_bytes)
    assert f1() == f2() == [1, 2, 3]

    # Check serialize after async resolve
    f3: StoreFactory[Any, list[int]] = StoreFactory(
        key,
        store_config=store.config(),
    )
    f3.resolve_async()
    f3_bytes = serialize(f3)
    f4 = deserialize(f3_bytes)
    assert f3() == f4() == [1, 2, 3]


def test_store_proxy(store: Store[LocalConnector]) -> None:
    """Test Store Proxy."""
    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    assert isinstance(p, Proxy)

    # Check that we can get the associated store back
    s = get_store(p)
    assert s is not None
    assert s.name == store.name

    assert p == [1, 2, 3]
    key = get_key(p)
    assert key is not None
    assert store.get(key) == [1, 2, 3]

    p = store.proxy_from_key(key)
    assert p == [1, 2, 3]

    p = store.proxy([2, 3, 4])
    key = get_key(p)
    assert key is not None
    assert store.get(key) == [2, 3, 4]

    with pytest.raises(TypeError):
        # String will not be serialized and should raise error when putting
        # array into Redis
        store.proxy('mystring', serializer=lambda s: s)

    assert isinstance(store.locked_proxy([1, 2, 3]), ProxyLocker)


def test_store_proxy_resolve_none_type(store: Store[LocalConnector]) -> None:
    # We have to put None in the store first then create a proxy from the
    # key because store.proxy(None) will just return None as a shortcut
    # because it is a singleton type.
    key = store.put(None)
    p: Proxy[None] = store.proxy_from_key(key)
    assert isinstance(p, Proxy)
    assert isinstance(p, type(None))


def test_nonproxiable_types(store: Store[LocalConnector]) -> None:
    for t in (None, True, False):
        p = store.proxy(t, skip_nonproxiable=True)
        assert not isinstance(p, Proxy)
        assert p is t

        with pytest.raises(NonProxiableTypeError):
            store.proxy(t, skip_nonproxiable=False)


def test_proxy_recreates_store() -> None:
    """Test Proxy Recreates Store."""
    with Store('test', LocalConnector(), cache_size=0) as store:
        register_store(store)

        p: Proxy[list[int]] = store.proxy([1, 2, 3])
        key = get_key(p)
        assert key is not None

        # Unregister store so proxy recreates it when resolved
        unregister_store(store)

        # Resolve the proxy
        assert p == [1, 2, 3]

        # The store that created the proxy had cache_size=0 so the restored
        # store should also have cache_size=0.
        s = get_store(store.name)
        assert store.cache.maxsize == 0
        assert s is not None
        assert not s.is_cached(key)

        unregister_store(store)


def test_proxy_batch(store: Store[LocalConnector]) -> None:
    """Test Batch Creation of Proxies."""
    values1 = [b'test_value1', b'test_value2', b'test_value3']
    proxies1: list[Proxy[bytes]] = store.proxy_batch(
        values1,
        serializer=lambda s: s,
        deserializer=lambda s: s,
    )
    for p1, v1 in zip(proxies1, values1):
        assert p1 == v1

    values2 = ['test_value1', 'test_value2', 'test_value3']

    proxies2: list[Proxy[str]] = store.proxy_batch(values2)
    for p2, v2 in zip(proxies2, values2):
        assert p2 == v2


def test_batch_nonproxiable_types(store: Store[LocalConnector]) -> None:
    with pytest.raises(NonProxiableTypeError):
        # Only one bad value needed to fail
        store.proxy_batch(['string', None, 'string'], skip_nonproxiable=False)

    v1 = store.proxy_batch([None, True, False], skip_nonproxiable=True)
    assert all(not isinstance(v, Proxy) for v in v1)

    # Test mixed proxies and constants
    inputs = [None, 'string', False, True, [1, 2, 3], 'string']
    should_proxy = [False, True, False, False, True, True]
    v2 = store.proxy_batch(inputs, skip_nonproxiable=True)
    assert all(isinstance(v, Proxy) == e for v, e in zip(v2, should_proxy))


def test_locked_nonproxiable_types(store: Store[LocalConnector]) -> None:
    with pytest.raises(NonProxiableTypeError):
        store.locked_proxy(None, skip_nonproxiable=False)

    p = store.locked_proxy(None, skip_nonproxiable=True)
    assert not isinstance(p, Proxy)
    assert p is None


def test_raises_missing_key(store: Store[LocalConnector]) -> None:
    """Test Proxy/Factory raise missing key error."""
    proxy = store.proxy([1, 2, 3])
    key = get_key(proxy)
    store.evict(key)
    assert not store.exists(key)

    with pytest.raises(ProxyResolveMissingKeyError):
        proxy.__factory__.resolve()

    proxy = store.proxy_from_key(key=key)
    with pytest.raises(ProxyResolveMissingKeyError):
        proxy()

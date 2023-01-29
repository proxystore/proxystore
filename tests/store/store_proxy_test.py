"""Store Factory and Proxy Tests for Store Subclasses."""
from __future__ import annotations

from typing import NamedTuple

import pytest

from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import deserialize
from proxystore.serialize import serialize
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import StoreFactory
from proxystore.store.exceptions import ProxyResolveMissingKeyError
from proxystore.store.local import LocalStore
from proxystore.store.utils import get_key
from testing.stores import StoreFixtureType


def test_store_factory(store_implementation: StoreFixtureType) -> None:
    """Test Store Factory."""
    _, store_info = store_implementation

    store = store_info.type(
        store_info.name,
        cache_size=16,
        **store_info.kwargs,
    )
    register_store(store)

    key = store.set([1, 2, 3])

    # Clear store to see if factory can reinitialize it
    unregister_store(store_info.name)

    f: StoreFactory[NamedTuple, list[int]] = StoreFactory(
        key,
        store_info.type,
        store_info.name,
        store_kwargs=store_info.kwargs,
    )
    assert f() == [1, 2, 3]

    f2: StoreFactory[NamedTuple, list[int]] = StoreFactory(
        key,
        store_info.type,
        store_info.name,
        store_kwargs=store_info.kwargs,
        evict=True,
    )
    assert store.exists(key)
    assert f2() == [1, 2, 3]
    assert not store.exists(key)

    key = store.set([1, 2, 3])
    # Clear store to see if factory can reinitialize it
    unregister_store(store_info.name)

    f = StoreFactory(
        key,
        store_info.type,
        store_info.name,
        store_kwargs=store_info.kwargs,
    )
    f.resolve_async()
    assert f._obj_future is not None
    assert f() == [1, 2, 3]
    assert f._obj_future is None

    # Calling resolve_async should be no-op since value cached
    f.resolve_async()
    assert f._obj_future is None
    assert f() == [1, 2, 3]

    f_str = serialize(f)
    f = deserialize(f_str)
    assert f() == [1, 2, 3]

    class _MyStore(LocalStore):
        pass

    # Test raise error if store_name corresponds to store that does not
    # match the type specified in the StoreFactory
    f = StoreFactory(
        key,
        _MyStore,
        store_info.name,
        store_kwargs=store_info.kwargs,
    )
    with pytest.raises(ValueError, match='store of type'):
        f()

    unregister_store(store_info.name)


def test_store_proxy(store_implementation: StoreFixtureType) -> None:
    """Test Store Proxy."""
    store, store_info = store_implementation

    register_store(store)

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    assert isinstance(p, Proxy)

    # Check that we can get the associated store back
    s = get_store(p)
    assert s is not None and s.name == store.name

    assert p == [1, 2, 3]
    key = get_key(p)
    assert key is not None and store.get(key) == [1, 2, 3]

    p = store.proxy_from_key(key)
    assert p == [1, 2, 3]

    p = store.proxy([2, 3, 4])
    key = get_key(p)
    assert key is not None and store.get(key) == [2, 3, 4]

    with pytest.raises(TypeError):
        # String will not be serialized and should raise error when putting
        # array into Redis
        store.proxy('mystring', serializer=lambda s: s)

    assert isinstance(store.locked_proxy([1, 2, 3]), ProxyLocker)

    unregister_store(store_info.name)


def test_proxy_recreates_store(store_implementation: StoreFixtureType) -> None:
    """Test Proxy Recreates Store."""
    store, store_info = store_implementation

    register_store(store)

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None

    # Unregister store so proxy recreates it when resolved
    unregister_store(store_info.name)

    # Resolve the proxy
    assert p == [1, 2, 3]

    # The store that created the proxy had cache_size=0 so the restored
    # store should also have cache_size=0.
    s = get_store(store_info.name)
    assert store._cache.maxsize == 0
    assert s is not None and not s.is_cached(key)

    unregister_store(store_info.name)


def test_proxy_batch(store_implementation: StoreFixtureType) -> None:
    """Test Batch Creation of Proxies."""
    store, store_info = store_implementation

    register_store(store)

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

    unregister_store(store_info.name)


def test_raises_missing_key(store_implementation: StoreFixtureType) -> None:
    """Test Proxy/Factory raise missing key error."""
    store, store_info = store_implementation

    register_store(store)

    proxy = store.proxy([1, 2, 3])
    key = get_key(proxy)
    store.evict(key)
    assert not store.exists(key)

    with pytest.raises(ProxyResolveMissingKeyError):
        proxy.__factory__.resolve()

    proxy = store.proxy_from_key(key=key)
    with pytest.raises(ProxyResolveMissingKeyError):
        proxy()

    unregister_store(store_info.name)

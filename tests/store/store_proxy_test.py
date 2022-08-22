"""Store Factory and Proxy Tests for Store Subclasses."""
from __future__ import annotations

from typing import NamedTuple

import pytest

import proxystore as ps
from proxystore.proxy import Proxy
from proxystore.store.base import StoreFactory
from proxystore.store.exceptions import ProxyResolveMissingKey
from proxystore.store.local import LocalStore
from proxystore.store.utils import get_key
from testing.store_utils import FIXTURE_LIST


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_factory(store_fixture, request) -> None:
    """Test Store Factory."""
    store_config = request.getfixturevalue(store_fixture)

    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
    )

    key = store.set([1, 2, 3])

    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}
    f: StoreFactory[NamedTuple, list[int]] = StoreFactory(
        key,
        store_config.type,
        store_config.name,
        store_kwargs=store_config.kwargs,
    )
    assert f() == [1, 2, 3]

    f2: StoreFactory[NamedTuple, list[int]] = StoreFactory(
        key,
        store_config.type,
        store_config.name,
        store_kwargs=store_config.kwargs,
        evict=True,
    )
    assert store.exists(key)
    assert f2() == [1, 2, 3]
    assert not store.exists(key)

    key = store.set([1, 2, 3])
    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}
    f = StoreFactory(
        key,
        store_config.type,
        store_config.name,
        store_kwargs=store_config.kwargs,
    )
    f.resolve_async()
    assert f._obj_future is not None
    assert f() == [1, 2, 3]
    assert f._obj_future is None

    # Calling resolve_async should be no-op since value cached
    f.resolve_async()
    assert f._obj_future is None
    assert f() == [1, 2, 3]

    f_str = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_str)
    assert f() == [1, 2, 3]

    class _MyStore(LocalStore):
        pass

    # Test raise error if store_name corresponds to store that does not
    # match the type specified in the StoreFactory
    f = StoreFactory(
        key,
        _MyStore,
        store_config.name,
        store_kwargs=store_config.kwargs,
    )
    with pytest.raises(ValueError, match='store of type'):
        f()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_proxy(store_fixture, request) -> None:
    """Test Store Proxy."""
    store_config = request.getfixturevalue(store_fixture)

    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
        cache_size=0,
    )

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    # Check that we can get the associated store back
    s = ps.store.get_store(p)
    assert s is not None and s.name == store.name

    assert p == [1, 2, 3]
    key = get_key(p)
    assert key is not None and store.get(key) == [1, 2, 3]

    p = store.proxy_from_key(key)
    assert p == [1, 2, 3]

    p = store.proxy([2, 3, 4])
    key = get_key(p)
    assert key is not None and store.get(key) == [2, 3, 4]

    with pytest.raises(Exception):
        # String will not be serialized and should raise error when putting
        # array into Redis
        store.proxy('mystring', serialize=False)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_proxy_recreates_store(store_fixture, request) -> None:
    """Test Proxy Recreates Store."""
    store_config = request.getfixturevalue(store_fixture)

    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
        cache_size=0,
    )

    p: Proxy[list[int]] = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None

    # Force delete store so proxy recreates it when resolved
    ps.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]

    # The store that created the proxy had cache_size=0 so the restored
    # store should also have cache_size=0.
    s = ps.store.get_store(store_config.name)
    assert s is not None and not s.is_cached(key)

    # Repeat above but with cache_size=1
    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
        cache_size=1,
    )
    p = store.proxy([1, 2, 3])
    key = get_key(p)
    assert key is not None
    ps.store._stores = {}
    assert p == [1, 2, 3]
    s = ps.store.get_store(store_config.name)
    assert s is not None and s.is_cached(key)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_proxy_batch(store_fixture, request) -> None:
    """Test Batch Creation of Proxies."""
    store_config = request.getfixturevalue(store_fixture)

    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
    )

    values1 = [b'test_value1', b'test_value2', b'test_value3']
    proxies1: list[Proxy[bytes]] = store.proxy_batch(values1, serialize=False)
    for p1, v1 in zip(proxies1, values1):
        assert p1 == v1

    values2 = ['test_value1', 'test_value2', 'test_value3']

    proxies2: list[Proxy[str]] = store.proxy_batch(values2)
    for p2, v2 in zip(proxies2, values2):
        assert p2 == v2

    store.close()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_raises_missing_key(store_fixture, request) -> None:
    """Test Proxy/Factory raise missing key error."""
    store_config = request.getfixturevalue(store_fixture)

    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
    )

    proxy = store.proxy([1, 2, 3])
    key = get_key(proxy)
    store.evict(key)
    assert not store.exists(key)

    with pytest.raises(ProxyResolveMissingKey):
        proxy.__factory__.resolve()

    proxy = store.proxy_from_key(key=key)
    with pytest.raises(ProxyResolveMissingKey):
        proxy()

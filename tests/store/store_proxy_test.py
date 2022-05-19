"""Store Factory and Proxy Tests for Store Subclasses."""
from __future__ import annotations

import os
import shutil

from pytest import fixture
from pytest import mark
from pytest import raises

import proxystore as ps
from proxystore.store.base import StoreFactory
from proxystore.store.exceptions import ProxyResolveMissingKey
from testing.store_utils import FILE_DIR
from testing.store_utils import FILE_STORE
from testing.store_utils import GLOBUS_STORE
from testing.store_utils import mock_third_party_libs
from testing.store_utils import REDIS_STORE


@fixture(scope='session', autouse=True)
def init():
    """Set up test environment."""
    mpatch = mock_third_party_libs()
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)  # pragma: no cover
    yield mpatch
    mpatch.undo()
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)  # pragma: no cover


@mark.parametrize('store_config', [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_store_factory(store_config) -> None:
    """Test Store Factory."""
    store = ps.store.init_store(
        store_config['type'],
        store_config['name'],
        **store_config['kwargs'],
    )

    key = store.set([1, 2, 3])

    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}
    f = StoreFactory(
        key,
        store_config['type'],
        store_config['name'],
        store_kwargs=store_config['kwargs'],
    )
    assert f() == [1, 2, 3]

    f2 = StoreFactory(
        key,
        store_config['type'],
        store_config['name'],
        store_kwargs=store_config['kwargs'],
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
        store_config['type'],
        store_config['name'],
        store_kwargs=store_config['kwargs'],
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

    # Test raise error if we pass different store type to factory
    ps.store.init_store('LOCAL', name='local')
    f = StoreFactory(
        key,
        store_config['type'],
        'local',
        store_kwargs=store_config['kwargs'],
    )
    with raises(ValueError):
        f()


@mark.parametrize('store_config', [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_store_proxy(store_config) -> None:
    """Test Store Proxy."""
    store = ps.store.init_store(
        store_config['type'],
        store_config['name'],
        **store_config['kwargs'],
        cache_size=0,
    )

    p = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    # Check that we can get the associated store back
    assert ps.store.get_store(p).name == store.name

    assert p == [1, 2, 3]
    assert store.get(ps.proxy.get_key(p)) == [1, 2, 3]

    p2 = store.proxy(key=ps.proxy.get_key(p))
    assert p2 == [1, 2, 3]

    p = store.proxy([2, 3, 4])
    key = ps.proxy.get_key(p)
    assert store.get(key=key) == [2, 3, 4]

    with raises(ValueError):
        # At least one of key or object must be passed
        store.proxy()

    with raises(Exception):
        # String will not be serialized and should raise error when putting
        # array into Redis
        store.proxy('mystring', serialize=False)


@mark.parametrize('store_config', [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_proxy_recreates_store(store_config) -> None:
    """Test Proxy Recreates Store."""
    store = ps.store.init_store(
        store_config['type'],
        store_config['name'],
        **store_config['kwargs'],
        cache_size=0,
    )

    p = store.proxy([1, 2, 3])
    key = ps.proxy.get_key(p)

    # Force delete store so proxy recreates it when resolved
    ps.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]

    # The store that created the proxy had cache_size=0 so the restored
    # store should also have cache_size=0.
    assert not ps.store.get_store(store_config['name']).is_cached(key)

    # Repeat above but with cache_size=1
    store = ps.store.init_store(
        store_config['type'],
        store_config['name'],
        **store_config['kwargs'],
        cache_size=1,
    )
    p = store.proxy([1, 2, 3])
    key = ps.proxy.get_key(p)
    ps.store._stores = {}
    assert p == [1, 2, 3]
    assert ps.store.get_store(store_config['name']).is_cached(key)


@mark.parametrize('store_config', [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_proxy_batch(store_config) -> None:
    """Test Batch Creation of Proxies."""
    store = ps.store.init_store(
        store_config['type'],
        store_config['name'],
        **store_config['kwargs'],
    )

    with raises(ValueError):
        store.proxy_batch(None, keys=None)

    values = [b'test_value1', b'test_value2', b'test_value3']

    proxies = store.proxy_batch(values, serialize=False)
    for p, v in zip(proxies, values):
        assert p == v

    values = ['test_value1', 'test_value2', 'test_value3']

    proxies = store.proxy_batch(values)
    for p, v in zip(proxies, values):
        assert p == v

    proxies = store.proxy_batch(keys=[ps.proxy.get_key(p) for p in proxies])
    for p, v in zip(proxies, values):
        assert p == v

    store.close()


@mark.parametrize('store_config', [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_raises_missing_key(store_config) -> None:
    """Test Proxy/Factory raise missing key error."""
    store = ps.store.init_store(
        store_config['type'],
        store_config['name'],
        **store_config['kwargs'],
    )

    key = 'test_key'
    assert not store.exists(key)

    factory = StoreFactory(
        key,
        store_config['type'],
        store_config['name'],
        store_config['kwargs'],
    )
    with raises(ProxyResolveMissingKey):
        factory.resolve()

    proxy = store.proxy(key=key)
    with raises(ProxyResolveMissingKey):
        proxy()

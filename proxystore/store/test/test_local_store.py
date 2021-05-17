"""LocalStore Unit Tests"""
import numpy as np

from pytest import raises

import proxystore as ps
from proxystore.store.local import LocalStore
from proxystore.store.local import LocalFactory


def test_local_store_init() -> None:
    """Test LocalStore Initialization"""
    LocalStore(name='local')

    ps.store.init_store('local', name='local')


def test_local_store_base() -> None:
    """Test LocalStore Base Functionality"""
    store = LocalStore(name='local')
    value = 'test_value'

    # LocalStore.set()
    store.set('key_bytes', str.encode(value))
    store.set('key_str', value)
    store.set('key_callable', lambda: value)
    store.set('key_numpy', np.array([1, 2, 3]))

    # LocalStore.get()
    assert store.get('key_bytes') == str.encode(value)
    assert store.get('key_str') == value
    assert store.get('key_callable').__call__() == value
    assert store.get('key_fake') is None
    assert store.get('key_fake', default='alt_value') == 'alt_value'
    assert np.array_equal(store.get('key_numpy'), np.array([1, 2, 3]))

    # LocalStore.exists()
    assert store.exists('key_bytes')
    assert store.exists('key_str')
    assert store.exists('key_callable')
    assert not store.exists('key_fake')

    # LocalStore.is_cached()
    assert store.is_cached('key_bytes')
    assert store.is_cached('key_str')
    assert store.is_cached('key_callable')
    assert not store.is_cached('key_fake')

    # LocalStore.evict()
    store.evict('key_str')
    assert not store.exists('key_str')
    assert not store.is_cached('key_str')
    store.evict('key_fake')


def test_local_factory() -> None:
    """Test LocalFactory"""
    f = LocalFactory('key', name='local')
    # Force delete LocalStore backend if it exists so resolving factory
    # raises not initialized error
    ps.store._stores = {}
    with raises(RuntimeError):
        f()

    store = ps.store.init_store(ps.store.STORES.LOCAL, 'local')

    store.set('key', [1, 2, 3])
    f = LocalFactory('key', name='local')
    assert f() == [1, 2, 3]

    f2 = LocalFactory('key', name='local', evict=True)
    assert store.exists('key')
    assert f2() == [1, 2, 3]
    assert not store.exists('key')

    store.set('key', [1, 2, 3])
    f = LocalFactory('key', name='local')
    f.resolve_async()
    assert f() == [1, 2, 3]

    f_str = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_str)
    assert f() == [1, 2, 3]


def test_local_store_proxy() -> None:
    """Test LocalStore Proxying"""
    store = ps.store.init_store(ps.store.STORES.LOCAL, 'local')

    p = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    assert p == [1, 2, 3]
    assert store.get(ps.proxy.get_key(p)) == [1, 2, 3]

    p2 = store.proxy(key=ps.proxy.get_key(p))
    assert p2 == [1, 2, 3]

    store.proxy([2, 3, 4], 'key')
    assert store.get(key='key') == [2, 3, 4]

    with raises(ValueError):
        # At least one of key or object must be passed
        store.proxy()

    with raises(ValueError):
        # Cannot make proxy from key that does not exist
        store.proxy(key='missing_key')

"""Base Store Unit Tests"""
import numpy as np

from pytest import raises

import proxystore as ps
from proxystore.backend import init_local_backend
from proxystore.backend.store import BaseStore, LocalStore, CachedStore

REDIS_HOST = 'localhost'
REDIS_PORT = 59465


def test_init_local_backend() -> None:
    """Test init_local_backend"""
    init_local_backend()
    assert ps.store is not None
    assert isinstance(ps.store, BaseStore)
    assert isinstance(ps.store, LocalStore)
    store = ps.store

    # Calling init again should do nothing since we already
    # have a Redis backend initialized
    init_local_backend()
    assert store is ps.store

    ps.store = BaseStore()

    # Should raise error that a different backend is already used
    with raises(ValueError):
        init_local_backend()


def test_local_store_basic() -> None:
    """Test LocalStore backend"""
    store = LocalStore()

    # Set various object types
    value = 'test_value'
    store.set('key_bytes', str.encode(value))
    store.set('key_str', value)
    store.set('key_callable', lambda: value)
    store.set('key_numpy', np.array([1, 2, 3]))

    # Get
    assert store.get('key_bytes') == str.encode(value)
    assert store.get('key_str') == value
    assert store.get('key_callable').__call__() == value
    assert store.get('key_fake') is None
    assert np.array_equal(store.get('key_numpy'), np.array([1, 2, 3]))

    # All keys should exists but all should be cached since the store
    # exists in LocalMemory
    assert store.exists('key_bytes')
    assert store.exists('key_str')
    assert store.exists('key_callable')
    assert store.is_cached('key_bytes')
    assert store.is_cached('key_str')
    assert store.is_cached('key_callable')

    # Test eviction
    store.evict('key_str')
    assert not store.exists('key_str')
    assert not store.is_cached('key_str')

    # Clear rest of keys from Redis for future tests
    store.evict('key_bytes')
    store.evict('key_callable')
    store.evict('key_numpy')

    # This key does not exists but no errors should be raised
    store.evict('key_fake')


def test_cached_store() -> None:
    """Test Cached Store"""
    CachedStore(1)
    with raises(ValueError):
        CachedStore(-1)

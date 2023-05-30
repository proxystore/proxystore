"""Store Base Functionality Tests."""
from __future__ import annotations

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.store import Store


def test_negative_cache_size() -> None:
    with pytest.raises(ValueError):
        Store('test', LocalConnector(), cache_size=-1)


def test_store_base(store: Store[LocalConnector]) -> None:
    """Test Store Base Functionality."""
    value = 'test_value'

    # Store.put()
    key_bytes = store.put(str.encode(value))
    key_str = store.put(value)
    key_callable = store.put(lambda: value)
    key_array = store.put([1, 2, 3])

    key_fake = store.put(None)
    store.evict(key_fake)

    # Store.get()
    assert store.get(key_bytes) == str.encode(value)
    assert store.get(key_str) == value
    c = store.get(key_callable)
    assert c is not None
    assert c.__call__() == value
    assert store.get(key_fake) is None
    assert store.get(key_fake, default='alt_value') == 'alt_value'
    assert store.get(key_array) == [1, 2, 3]

    # Store.exists()
    assert store.exists(key_bytes)
    assert store.exists(key_str)
    assert store.exists(key_callable)
    assert not store.exists(key_fake)

    # Store.evict()
    store.evict(key_str)
    assert not store.exists(key_str)
    assert not store.is_cached(key_str)
    store.evict(key_fake)


def test_store_caching() -> None:
    """Test Store Caching Functionality."""
    with Store('test', LocalConnector(), cache_size=0) as store:
        assert store.cache.maxsize == 0
        value = 'test_value'

        # Test cache size 0
        key1 = store.put(value)
        assert store.get(key1) == value
        assert not store.is_cached(key1)

    with Store('test', LocalConnector(), cache_size=1) as store:
        # Add our test value
        key1 = store.put(value)

        # Test caching
        assert not store.is_cached(key1)
        # Cache exists is false but this is still true
        assert store.exists(key1)
        assert store.get(key1) == value
        assert store.is_cached(key1)
        # Cache exists is true shortcut
        assert store.exists(key1)

        # Add second value
        key2 = store.put(value)
        assert store.is_cached(key1)
        assert not store.is_cached(key2)

        # Check cached value flipped since cache size is 1
        assert store.get(key2) == value
        assert not store.is_cached(key1)
        assert store.is_cached(key2)


def test_store_custom_serialization(store: Store[LocalConnector]) -> None:
    """Test store custom serialization."""
    # Pretend serialized string
    s = b'ABC'
    key = store.put(s, serializer=lambda s: s)
    assert store.get(key, deserializer=lambda s: s) == s

    with pytest.raises(TypeError, match='bytes'):
        # Should fail because the array is not already serialized
        store.put([1, 2, 3], serializer=lambda s: s)

    with pytest.raises(TypeError, match='bytes'):
        # Should fail because the array is not already serialized
        store.put_batch([[1, 2, 3]], serializer=lambda s: s)


def test_store_batch_ops(store: Store[LocalConnector]) -> None:
    """Test batch operations."""
    values = ['test_value1', 'test_value2', 'test_value3']

    # Test without keys
    keys = store.put_batch(values)
    for key in keys:
        assert store.exists(key)


def test_store_batch_ops_remote(store: Store[LocalConnector]) -> None:
    """Test batch operations with custom serialization."""
    values = ['test_value1', 'test_value2', 'test_value3']

    new_keys = store.put_batch(values, serializer=lambda s: str.encode(s))
    for key in new_keys:
        assert store.exists(key)

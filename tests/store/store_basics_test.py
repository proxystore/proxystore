"""Store Base Functionality Tests."""
from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from proxystore.store.cache import LRUCache
from testing.stores import missing_key
from testing.stores import StoreFixtureType


def test_store_init(store_implementation: StoreFixtureType) -> None:
    """Test Store Base Functionality."""
    _, store_info = store_implementation

    with pytest.raises(ValueError):
        # Negative Cache Size Error
        store_info.type(store_info.name, **store_info.kwargs, cache_size=-1)


def test_store_base(store_implementation: StoreFixtureType) -> None:
    """Test Store Base Functionality."""
    store, store_info = store_implementation

    key_fake = missing_key(store)
    value = 'test_value'

    # Store.set()
    key_bytes = store.set(str.encode(value))
    key_str = store.set(value)
    key_callable = store.set(lambda: value)
    key_array = store.set([1, 2, 3])

    # Store.get()
    assert store.get(key_bytes) == str.encode(value)
    assert store.get(key_str) == value
    c = store.get(key_callable)
    assert c is not None and c.__call__() == value
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


def test_store_caching(store_implementation: StoreFixtureType) -> None:
    """Test Store Caching Functionality."""
    store, _ = store_implementation

    assert store._cache.maxsize == 0
    value = 'test_value'

    # Test cache size 0
    key1 = store.set(value)
    assert store.get(key1) == value
    assert not store.is_cached(key1)

    # Manually change cache size to size 1
    new_cache: LRUCache[str, Any] = LRUCache(1)
    with mock.patch.object(store, '_cache', new_cache):
        # Add our test value
        key1 = store.set(value)

        # Test caching
        assert not store.is_cached(key1)
        assert store.get(key1) == value
        assert store.is_cached(key1)

        # Add second value
        key2 = store.set(value)
        assert store.is_cached(key1)
        assert not store.is_cached(key2)

        # Check cached value flipped since cache size is 1
        assert store.get(key2) == value
        assert not store.is_cached(key1)
        assert store.is_cached(key2)


def test_store_custom_serialization(
    store_implementation: StoreFixtureType,
) -> None:
    """Test store custom serialization."""
    store, store_info = store_implementation

    # Pretend serialized string
    s = b'ABC'
    key = store.set(s, serializer=lambda s: s)
    assert store.get(key, deserializer=lambda s: s) == s

    with pytest.raises(TypeError, match='bytes'):
        # Should fail because the array is not already serialized
        store.set([1, 2, 3], serializer=lambda s: s)


def test_store_batch_ops(store_implementation: StoreFixtureType) -> None:
    """Test batch operations."""
    store, store_info = store_implementation

    values = ['test_value1', 'test_value2', 'test_value3']

    # Test without keys
    keys = store.set_batch(values)
    for key in keys:
        assert store.exists(key)


def test_store_batch_ops_remote(
    store_implementation: StoreFixtureType,
) -> None:
    """Test batch operations with custom serialization."""
    store, store_info = store_implementation

    values = ['test_value1', 'test_value2', 'test_value3']

    new_keys = store.set_batch(values, serializer=lambda s: str.encode(s))
    for key in new_keys:
        assert store.exists(key)

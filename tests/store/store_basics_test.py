"""Store Base Functionality Tests."""
from __future__ import annotations

import numpy as np
import pytest

import proxystore as ps
from proxystore.store.base import Store
from testing.store_utils import FIXTURE_LIST


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_init(store_fixture, request) -> None:
    """Test Store Base Functionality."""
    store_config = request.getfixturevalue(store_fixture)

    store_config.type(store_config.name, **store_config.kwargs)

    store = ps.store.init_store(
        store_config.type,
        store_config.name,
        **store_config.kwargs,
    )
    assert isinstance(store, Store)

    with pytest.raises(ValueError):
        # Negative Cache Size Error
        ps.store.init_store(
            store_config.type,
            store_config.name,
            **store_config.kwargs,
            cache_size=-1,
        )


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_base(store_fixture, request) -> None:
    """Test Store Base Functionality."""
    store_config = request.getfixturevalue(store_fixture)

    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
    )

    key_fake = 'key_fake'
    value = 'test_value'

    # Store.set()
    key_bytes = store.set(str.encode(value))
    key_str = store.set(value)
    key_callable = store.set(lambda: value)
    key_numpy = store.set(np.array([1, 2, 3]), key='key_numpy')

    # Store.get()
    assert store.get(key_bytes) == str.encode(value)
    assert store.get(key_str) == value
    assert store.get(key_callable).__call__() == value
    assert store.get(key_fake) is None
    assert store.get(key_fake, default='alt_value') == 'alt_value'
    assert np.array_equal(store.get(key_numpy), np.array([1, 2, 3]))

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

    store.close()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_caching(store_fixture, request) -> None:
    """Test Store Caching Functionality."""
    store_config = request.getfixturevalue(store_fixture)

    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
        cache_size=1,
    )

    # Add our test value
    value = 'test_value'
    base_key = 'base_key'
    assert not store.exists(base_key)
    key1 = store.set(value, key=base_key)

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

    # Now test cache size 0
    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
        cache_size=0,
    )
    key1 = store.set(value)
    assert store.get(key1) == value
    assert not store.is_cached(key1)

    store.close()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_timestamps(store_fixture, request) -> None:
    """Test Store Timestamps."""
    store_config = request.getfixturevalue(store_fixture)

    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
        cache_size=1,
    )

    missing_key = 'key12398908352'
    with pytest.raises(KeyError):
        store.get_timestamp(missing_key)

    key = store.set('timestamp_test_value')
    assert isinstance(store.get_timestamp(key), float)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_strict(store_fixture, request) -> None:
    """Test Store Strict Functionality."""
    store_config = request.getfixturevalue(store_fixture)

    if store_config.type.__name__ in ('GlobusStore', 'EndpointStore'):
        # GlobusStore/EndpointStore do not support strict guarantees
        return

    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
        cache_size=1,
    )

    # Add our test value
    value = 'test_value'
    base_key = 'strict_key'
    assert not store.exists(base_key)
    key = store.set(value, key=base_key)

    # Access key so value is cached locally
    assert store.get(key) == value
    assert store.is_cached(key)

    # Change value in Store
    key = store.set('new_value', key=base_key)
    # Old value of key is still cached
    assert store.get(key) == value
    assert store.is_cached(key)
    assert not store.is_cached(key, strict=True)

    # Access with strict=True so now most recent version should be cached
    assert store.get(key, strict=True) == 'new_value'
    assert store.get(key) == 'new_value'
    assert store.is_cached(key)
    assert store.is_cached(key, strict=True)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_custom_serialization(store_fixture, request) -> None:
    """Test Store Custom Serialization."""
    store_config = request.getfixturevalue(store_fixture)

    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
    )
    # Pretend serialized string
    s = b'ABC'
    key = store.set(s, serialize=False)
    assert store.get(key, deserialize=False) == s

    with pytest.raises(TypeError, match='bytes'):
        # Should fail because the numpy array is not already serialized
        store.set(np.array([1, 2, 3]), key=key, serialize=False)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_batch_ops(store_fixture, request) -> None:
    store_config = request.getfixturevalue(store_fixture)

    """Test Batch Operations."""
    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
    )

    keys = ['key1', 'key2', 'key3']
    values = ['test_value1', 'test_value2', 'test_value3']

    # Test without keys
    new_keys = store.set_batch(values)
    for key in new_keys:
        assert store.exists(key)

    # Test with keys
    new_keys = store.set_batch(values, keys=keys)
    for key in new_keys:
        assert store.exists(key)

    # Test length mismatch between values and keys
    with pytest.raises(ValueError):
        store.set_batch(values, keys=new_keys[:1])

    store.close()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_batch_ops_remote(store_fixture, request) -> None:
    """Test Batch Operations for Remote Stores."""
    store_config = request.getfixturevalue(store_fixture)

    store = store_config.type(
        store_config.name,
        **store_config.kwargs,
    )

    values = ['test_value1', 'test_value2', 'test_value3']

    new_keys = store.set_batch(values, serialize=True)
    for key in new_keys:
        assert store.exists(key)

    store.close()

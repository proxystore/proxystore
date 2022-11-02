"""Store Base Functionality Tests."""
from __future__ import annotations

import numpy as np
import pytest

import proxystore as ps
from proxystore.store.base import Store
from testing.store_utils import FIXTURE_LIST
from testing.store_utils import missing_key


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

    store.close()


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_base(store_fixture, request) -> None:
    """Test Store Base Functionality."""
    store_config = request.getfixturevalue(store_fixture)

    with store_config.type(store_config.name, **store_config.kwargs) as store:
        key_fake = missing_key(store)
        value = 'test_value'

        # Store.set()
        key_bytes = store.set(str.encode(value))
        key_str = store.set(value)
        key_callable = store.set(lambda: value)
        key_numpy = store.set(np.array([1, 2, 3]))

        # Store.get()
        assert store.get(key_bytes) == str.encode(value)
        assert store.get(key_str) == value
        c = store.get(key_callable)
        assert c is not None and c.__call__() == value
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


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_caching(store_fixture, request) -> None:
    """Test Store Caching Functionality."""
    store_config = request.getfixturevalue(store_fixture)

    with store_config.type(
        store_config.name,
        **store_config.kwargs,
        cache_size=1,
    ) as store:
        # Add our test value
        value = 'test_value'
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

        # Now test cache size 0
        store = store_config.type(
            store_config.name,
            **store_config.kwargs,
            cache_size=0,
        )
        key1 = store.set(value)
        assert store.get(key1) == value
        assert not store.is_cached(key1)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_custom_serialization(store_fixture, request) -> None:
    """Test Store Custom Serialization."""
    store_config = request.getfixturevalue(store_fixture)

    with store_config.type(store_config.name, **store_config.kwargs) as store:
        # Pretend serialized string
        s = b'ABC'
        key = store.set(s, serialize=False)
        assert store.get(key, deserialize=False) == s

        with pytest.raises(TypeError, match='bytes'):
            # Should fail because the numpy array is not already serialized
            store.set(np.array([1, 2, 3]), serialize=False)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_batch_ops(store_fixture, request) -> None:
    """Test Batch Operations."""
    store_config = request.getfixturevalue(store_fixture)

    with store_config.type(store_config.name, **store_config.kwargs) as store:
        values = ['test_value1', 'test_value2', 'test_value3']

        # Test without keys
        keys = store.set_batch(values)
        for key in keys:
            assert store.exists(key)


@pytest.mark.parametrize('store_fixture', FIXTURE_LIST)
def test_store_batch_ops_remote(store_fixture, request) -> None:
    """Test Batch Operations for Remote Stores."""
    store_config = request.getfixturevalue(store_fixture)

    with store_config.type(store_config.name, **store_config.kwargs) as store:
        values = ['test_value1', 'test_value2', 'test_value3']

        new_keys = store.set_batch(values, serialize=True)
        for key in new_keys:
            assert store.exists(key)

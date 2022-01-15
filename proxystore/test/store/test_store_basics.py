"""Store Base Functionality Tests."""
import os
import shutil

import numpy as np
from pytest import fixture
from pytest import mark
from pytest import raises

import proxystore as ps
from proxystore.store.base import Store
from proxystore.store.remote import RemoteStore
from proxystore.test.store.utils import FILE_DIR
from proxystore.test.store.utils import FILE_STORE
from proxystore.test.store.utils import GLOBUS_STORE
from proxystore.test.store.utils import LOCAL_STORE
from proxystore.test.store.utils import mock_third_party_libs
from proxystore.test.store.utils import REDIS_STORE


@fixture(scope="session", autouse=True)
def init() -> None:
    """Set up test environment."""
    mpatch = mock_third_party_libs()
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)
    yield mpatch
    mpatch.undo()
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)


@mark.parametrize(
    "store_config",
    [LOCAL_STORE, FILE_STORE, REDIS_STORE, GLOBUS_STORE],
)
def test_store_init(store_config) -> None:
    """Test Store Base Functionality."""
    store_config["type"](store_config["name"], **store_config["kwargs"])

    store = ps.store.init_store(
        store_config["type"],
        store_config["name"],
        **store_config["kwargs"],
    )
    assert isinstance(store, Store)

    if issubclass(store_config["type"], RemoteStore):
        with raises(ValueError):
            # Negative Cache Size Error
            ps.store.init_store(
                store_config["type"],
                store_config["name"],
                **store_config["kwargs"],
                cache_size=-1,
            )


@mark.parametrize(
    "store_config",
    [LOCAL_STORE, FILE_STORE, REDIS_STORE, GLOBUS_STORE],
)
def test_store_base(store_config) -> None:
    """Test Store Base Functionality."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
    )

    key_fake = "key_fake"
    value = "test_value"

    # Store.set()
    key_bytes = store.set(str.encode(value))
    key_str = store.set(value)
    key_callable = store.set(lambda: value)
    key_numpy = store.set(np.array([1, 2, 3]), key="key_numpy")

    # Store.get()
    assert store.get(key_bytes) == str.encode(value)
    assert store.get(key_str) == value
    assert store.get(key_callable).__call__() == value
    assert store.get(key_fake) is None
    assert store.get(key_fake, default="alt_value") == "alt_value"
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

    store.cleanup()


@mark.parametrize("store_config", [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_store_caching(store_config) -> None:
    """Test Store Caching Functionality."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
        cache_size=1,
    )

    # Add our test value
    value = "test_value"
    base_key = "base_key"
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
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
        cache_size=0,
    )
    key1 = store.set(value)
    assert store.get(key1) == value
    assert not store.is_cached(key1)

    store.cleanup()


@mark.parametrize("store_config", [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_store_timestamps(store_config) -> None:
    """Test Store Timestamps."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
        cache_size=1,
    )

    missing_key = "key12398908352"
    with raises(KeyError):
        store.get_timestamp(missing_key)

    key = store.set("timestamp_test_value")
    assert isinstance(store.get_timestamp(key), float)


@mark.parametrize("store_config", [FILE_STORE, REDIS_STORE])
def test_store_strict(store_config) -> None:
    """Test Store Strict Functionality."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
        cache_size=1,
    )

    # Add our test value
    value = "test_value"
    base_key = "strict_key"
    assert not store.exists(base_key)
    key = store.set(value, key=base_key)

    # Access key so value is cached locally
    assert store.get(key) == value
    assert store.is_cached(key)

    # Change value in Store
    key = store.set("new_value", key=base_key)
    # Old value of key is still cached
    assert store.get(key) == value
    assert store.is_cached(key)
    assert not store.is_cached(key, strict=True)

    # Access with strict=True so now most recent version should be cached
    assert store.get(key, strict=True) == "new_value"
    assert store.get(key) == "new_value"
    assert store.is_cached(key)
    assert store.is_cached(key, strict=True)


@mark.parametrize("store_config", [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_store_custom_serialization(store_config) -> None:
    """Test Store Custom Serialization."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
    )
    # Pretend serialized string
    s = b"ABC"
    key = store.set(s, serialize=False)
    assert store.get(key, deserialize=False) == s

    with raises(Exception):
        # Should fail because the numpy array is not already serialized
        store.set(np.array([1, 2, 3]), key=key, serialize=False)


@mark.parametrize(
    "store_config",
    [LOCAL_STORE, FILE_STORE, REDIS_STORE, GLOBUS_STORE],
)
def test_store_batch_ops(store_config) -> None:
    """Test Batch Operations."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
    )

    keys = ["key1", "key2", "key3"]
    values = ["test_value1", "test_value2", "test_value3"]

    # Test without keys
    new_keys = store.set_batch(values)
    for key in new_keys:
        assert store.exists(key)

    # Test with keys
    new_keys = store.set_batch(values, keys=keys)
    for key in new_keys:
        assert store.exists(key)

    # Test length mismatch between values and keys
    with raises(ValueError):
        store.set_batch(values, keys=new_keys[:1])

    store.cleanup()


@mark.parametrize("store_config", [FILE_STORE, REDIS_STORE, GLOBUS_STORE])
def test_store_batch_ops_remote(store_config) -> None:
    """Test Batch Operations for Remote Stores."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
    )

    values = ["test_value1", "test_value2", "test_value3"]

    # Subclasses of RemoteStore have an additional serialize parameter
    new_keys = store.set_batch(values, serialize=True)
    for key in new_keys:
        assert store.exists(key)

    store.cleanup()

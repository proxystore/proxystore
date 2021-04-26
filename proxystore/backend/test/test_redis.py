"""Redis Backend Unit Tests"""
import numpy as np
import os
import subprocess
import time

from pytest import raises, fixture

import proxystore as ps
from proxystore.backend.store import PROXYSTORE_CACHE_SIZE_ENV
from proxystore.backend.store import BaseStore, RedisStore
from proxystore.backend.serialize import SerializationError

REDIS_HOST = 'localhost'
REDIS_PORT = 59465


@fixture(scope='session', autouse=True)
def init() -> None:
    """Launch Redis Server for Tests"""
    redis_handle = subprocess.Popen(
        ['redis-server', '--port', str(REDIS_PORT)], stdout=subprocess.DEVNULL
    )
    time.sleep(1)
    yield
    redis_handle.kill()


def test_init_redis_backend() -> None:
    """Test init_redis_backend"""
    ps.store = None
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
    assert ps.store is not None
    assert isinstance(ps.store, BaseStore)
    assert isinstance(ps.store, RedisStore)
    store = ps.store

    # Calling init again should do nothing since we already
    # have a Redis backend initialized
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
    assert store is ps.store

    ps.store = BaseStore()

    # Should raise error that a different backend is already used
    with raises(ValueError):
        ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)


def test_redis_store_basic() -> None:
    """Test RedisStore backend"""
    store = RedisStore(hostname=REDIS_HOST, port=REDIS_PORT, cache_size=0)

    # Set various object types
    value = 'test_value'
    store.set('key_bytes', str.encode(value))
    store.set('key_str', value)
    # TODO(gpauloski): add serialization support
    # store.set('key_callable', lambda: value)
    store.set('key_numpy', np.array([1, 2, 3]))

    # Get
    assert store.get('key_bytes') == str.encode(value)
    assert store.get('key_str') == value
    # assert store.get('key_callable').__call__() == value
    assert store.get('key_fake') is None
    assert np.array_equal(store.get('key_numpy'), np.array([1, 2, 3]))

    # All keys should exists but none should be cached (cache_size = 0)
    assert store.exists('key_bytes')
    assert store.exists('key_str')
    # assert store.exists('key_callable')
    assert not store.is_cached('key_bytes')
    assert not store.is_cached('key_str')
    # assert not store.is_cached('key_callable')

    # Test eviction
    store.evict('key_str')
    assert not store.exists('key_str')
    assert not store.is_cached('key_str')

    # Test pre-serialized objects
    store.set('key_str', value, serialize=False)
    with raises(SerializationError):
        assert store.get('key_str') == value
    assert store.get('key_str', deserialize=False) == value

    # Clear rest of keys from Redis for future tests
    store.evict('key_bytes')
    store.evict('key_str')
    # store.evict('key_callable')
    store.evict('key_numpy')


def test_redis_store_caching() -> None:
    """Test RedisStore backend with caching"""
    os.environ[PROXYSTORE_CACHE_SIZE_ENV] = '1'
    store = RedisStore(hostname=REDIS_HOST, port=REDIS_PORT)

    # Add our test value to
    value = 'test_value'
    assert not store.exists('key')
    store.set('key', value)

    # Test caching
    assert not store.is_cached('key')
    assert store.get('key') == value
    assert store.is_cached('key')

    # Add second value
    store.set('key2', value)
    assert store.is_cached('key')
    assert not store.is_cached('key2')

    # Confirm cache size is actually 1 from env variable
    # Cached key-value should flip
    assert store.get('key2') == value
    assert not store.is_cached('key')
    assert store.is_cached('key2')

    # Clean up keys
    store.evict('key')
    store.evict('key2')


def test_redis_store_strict() -> None:
    """Test RedisStore backend strict guarentees"""
    store = RedisStore(hostname=REDIS_HOST, port=REDIS_PORT, cache_size=2)

    # Add our test value to
    value = 'test_value'
    assert not store.exists('key')
    store.set('key', value)

    # Access key so value is cached locally
    assert store.get('key') == value
    assert store.is_cached('key')

    # Change value in Redis
    store.set('key', 'new_value')
    assert store.get('key') == value
    assert store.is_cached('key')
    assert not store.is_cached('key', strict=True)

    # Access with strict=True so now most recent version should be cached
    assert store.get('key', strict=True) == 'new_value'
    assert store.get('key') == 'new_value'
    assert store.is_cached('key')
    assert store.is_cached('key', strict=True)

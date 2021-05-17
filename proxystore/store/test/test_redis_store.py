"""RedisStore Unit Tests"""
import numpy as np
import subprocess
import time

from pytest import fixture, raises

import proxystore as ps
from proxystore.store.redis import RedisStore
from proxystore.store.redis import RedisFactory

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


def test_redis_store_init() -> None:
    """Test RedisStore Initialization"""
    RedisStore('redis', REDIS_HOST, REDIS_PORT)

    ps.store.init_store(
        ps.store.STORES.REDIS, 'redis', hostname=REDIS_HOST, port=REDIS_PORT
    )

    with raises(ValueError):
        # Negative cache_size error
        ps.store.init_store(
            ps.store.STORES.REDIS,
            'redis',
            hostname=REDIS_HOST,
            port=REDIS_PORT,
            cache_size=-1,
        )


def test_redis_store_base() -> None:
    """Test RedisStore Base Functionality"""
    store = RedisStore('redis', REDIS_HOST, REDIS_PORT)
    value = 'test_value'

    # RedisStore.set()
    store.set('key_bytes', str.encode(value))
    store.set('key_str', value)
    store.set('key_callable', lambda: value)
    store.set('key_numpy', np.array([1, 2, 3]))

    # RedisStore.get()
    assert store.get('key_bytes') == str.encode(value)
    assert store.get('key_str') == value
    assert store.get('key_callable').__call__() == value
    assert store.get('key_fake') is None
    assert store.get('key_fake', default='alt_value') == 'alt_value'
    assert np.array_equal(store.get('key_numpy'), np.array([1, 2, 3]))

    # RedisStore.exists()
    assert store.exists('key_bytes')
    assert store.exists('key_str')
    assert store.exists('key_callable')
    assert not store.exists('key_fake')

    # RedisStore.is_cached()
    assert store.is_cached('key_bytes')
    assert store.is_cached('key_str')
    assert store.is_cached('key_callable')
    assert not store.is_cached('key_fake')

    # RedisStore.evict()
    store.evict('key_str')
    assert not store.exists('key_str')
    assert not store.is_cached('key_str')
    store.evict('key_fake')


def test_redis_store_caching() -> None:
    """Test RedisStore Caching"""
    store = RedisStore('redis', REDIS_HOST, REDIS_PORT, cache_size=1)

    # Add our test value
    value = 'test_value'
    assert not store.exists('cache_key')
    store.set('cache_key', value)

    # Test caching
    assert not store.is_cached('cache_key')
    assert store.get('cache_key') == value
    assert store.is_cached('cache_key')

    # Add second value
    store.set('cache_key2', value)
    assert store.is_cached('cache_key')
    assert not store.is_cached('cache_key2')

    # Check cached value flipped since cache size is 1
    assert store.get('cache_key2') == value
    assert not store.is_cached('cache_key')
    assert store.is_cached('cache_key2')

    # Now test cache size 0
    store = RedisStore('redis', REDIS_HOST, REDIS_PORT, cache_size=0)
    store.set('cache_key', value)
    assert store.get('cache_key') == value
    assert not store.is_cached('cache_key')


def test_redis_store_strict() -> None:
    """Test RedisStore Strict Guarentees"""
    store = RedisStore(
        'redis', hostname=REDIS_HOST, port=REDIS_PORT, cache_size=1
    )

    # Add our test value
    value = 'test_value'
    assert not store.exists('strict_key')
    store.set('strict_key', value)

    # Access key so value is cached locally
    assert store.get('strict_key') == value
    assert store.is_cached('strict_key')

    # Change value in Redis
    store.set('strict_key', 'new_value')
    assert store.get('strict_key') == value
    assert store.is_cached('strict_key')
    assert not store.is_cached('strict_key', strict=True)

    # Access with strict=True so now most recent version should be cached
    assert store.get('strict_key', strict=True) == 'new_value'
    assert store.get('strict_key') == 'new_value'
    assert store.is_cached('strict_key')
    assert store.is_cached('strict_key', strict=True)


def test_redis_store_custom_serialization() -> None:
    """Test RedisStore Custom Serialization"""
    store = RedisStore(
        'redis', hostname=REDIS_HOST, port=REDIS_PORT, cache_size=1
    )

    # Pretend serialized string
    s = 'ABC'
    store.set('serial_key', s, serialize=False)
    assert store.get('serial_key', deserialize=False) == s

    with raises(Exception):
        # Should fail because the numpy array is not already serialized
        store.set('serial_key', np.array([1, 2, 3]), serialize=False)


def test_redis_factory() -> None:
    """Test RedisFactory"""
    store = ps.store.init_store(
        ps.store.STORES.REDIS, 'redis', hostname=REDIS_HOST, port=REDIS_PORT
    )
    store.set('key', [1, 2, 3])

    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}
    f = RedisFactory('key', 'redis', REDIS_HOST, REDIS_PORT)
    assert f() == [1, 2, 3]

    f2 = RedisFactory('key', 'redis', REDIS_HOST, REDIS_PORT, evict=True)
    assert store.exists('key')
    assert f2() == [1, 2, 3]
    assert not store.exists('key')

    store.set('key', [1, 2, 3])
    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}
    f = RedisFactory('key', 'redis', REDIS_HOST, REDIS_PORT)
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


def test_redis_store_proxy() -> None:
    """Test RedisStore Proxying"""
    store = ps.store.init_store(
        ps.store.STORES.REDIS, 'redis', hostname=REDIS_HOST, port=REDIS_PORT
    )

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

    with raises(Exception):
        # Array will not be serialized and should raise error when putting
        # array into Redis
        store.proxy(np.ndarray([1, 2, 3]), serialize=False)


def test_proxy_recreates_store() -> None:
    """Test RedisStore Proxy with RedisFactory can Recreate the Store"""
    store = ps.store.init_store(
        'redis', 'redis', hostname=REDIS_HOST, port=REDIS_PORT, cache_size=0
    )

    p = store.proxy([1, 2, 3], key='recreate_key')

    # Force delete store so proxy recreates it when resolved
    ps.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]

    # The store that created the proxy had cache_size=0 so the restored
    # store should also have cache_size=0.
    assert not ps.store.get_store('redis').is_cached('recreate_key')

    # Repeat above but with cache_size=1
    store = ps.store.init_store(
        'redis', 'redis', hostname=REDIS_HOST, port=REDIS_PORT, cache_size=1
    )
    p = store.proxy([1, 2, 3], key='recreate_key')
    ps.store._stores = {}
    assert p == [1, 2, 3]
    assert ps.store.get_store('redis').is_cached('recreate_key')

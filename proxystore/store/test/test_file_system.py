"""FileStore Unit Tests"""
import numpy as np

from pytest import raises

import proxystore as ps
from proxystore.store.file import FileStore
from proxystore.store.file import FileFactory

STORE_DIR = '/tmp/proxystore'


def test_file_store_init() -> None:
    """Test FileStore Initialization"""
    FileStore('files', STORE_DIR)

    ps.store.init_store(ps.store.STORES.FILE, 'files', store_dir=STORE_DIR)

    with raises(ValueError):
        # Negative cache_size error
        ps.store.init_store(
            ps.store.STORES.FILE,
            'files',
            store_dir=STORE_DIR,
            cache_size=-1,
        )


def test_file_store_base() -> None:
    """Test FileStore Base Functionality"""
    store = FileStore('files', STORE_DIR)
    key_fake = 'key_fake'
    value = 'test_value'

    # FileStore.set()
    key_bytes = store.set(str.encode(value))
    key_str = store.set(value)
    key_callable = store.set(lambda: value)
    key_numpy = store.set(np.array([1, 2, 3]), key='key_numpy')
    assert key_numpy == 'key_numpy'

    # FileStore.get()
    assert store.get(key_bytes) == str.encode(value)
    assert store.get(key_str) == value
    assert store.get(key_callable).__call__() == value
    assert store.get(key_fake) is None
    assert store.get(key_fake, default='alt_value') == 'alt_value'
    assert np.array_equal(store.get(key_numpy), np.array([1, 2, 3]))

    # FileStore.exists()
    assert store.exists(key_bytes)
    assert store.exists(key_str)
    assert store.exists(key_callable)
    assert not store.exists(key_fake)

    # FileStore.is_cached()
    assert store.is_cached(key_bytes)
    assert store.is_cached(key_str)
    assert store.is_cached(key_callable)
    assert not store.is_cached(key_fake)

    # FileStore.evict()
    store.evict(key_str)
    assert not store.exists(key_str)
    assert not store.is_cached(key_str)
    store.evict(key_fake)

    store.cleanup()


def test_file_store_caching() -> None:
    """Test FileStore Caching"""
    store = FileStore('files', STORE_DIR, cache_size=1)

    # Add our test value
    value = 'test_value'
    key1 = 'cache_key'
    assert not store.exists(key1)
    store.set(value, key=key1)

    # Test caching
    assert not store.is_cached(key1)
    assert store.get(key1) == value
    assert store.is_cached(key1)

    # Add second value
    key2 = 'cache_key2'
    store.set(value, key=key2)
    assert store.is_cached(key1)
    assert not store.is_cached(key2)

    # Check cached value flipped since cache size is 1
    assert store.get(key2) == value
    assert not store.is_cached(key1)
    assert store.is_cached(key2)

    # Now test cache size 0
    store = FileStore('files', STORE_DIR, cache_size=0)
    store.set(value, key=key1)
    assert store.get(key1) == value
    assert not store.is_cached(key1)

    store.cleanup()


def test_file_store_strict() -> None:
    """Test FileStore Strict Guarentees"""
    store = FileStore('files', STORE_DIR, cache_size=1)

    # Add our test value
    value = 'test_value'
    key = 'strict_key'
    assert not store.exists(key)
    store.set(value, key=key)

    # Access key so value is cached locally
    assert store.get(key) == value
    assert store.is_cached(key)

    # Change value in Redis
    store.set('new_value', key=key)
    assert store.get(key) == value
    assert store.is_cached(key)
    assert not store.is_cached(key, strict=True)

    # Access with strict=True so now most recent version should be cached
    assert store.get(key, strict=True) == 'new_value'
    assert store.get(key) == 'new_value'
    assert store.is_cached(key)
    assert store.is_cached(key, strict=True)

    store.cleanup()


def test_file_store_custom_serialization() -> None:
    """Test FileStore Custom Serialization"""
    store = FileStore('files', STORE_DIR, cache_size=1)

    # Pretend serialized string
    s = 'ABC'
    key = 'serial_key'
    store.set(s, key=key, serialize=False)
    assert store.get(key, deserialize=False) == s

    with raises(Exception):
        # Should fail because the numpy array is not already serialized
        store.set(key, np.array([1, 2, 3]), serialize=False)

    store.cleanup()


def test_file_factory() -> None:
    """Test FileFactory"""
    store = ps.store.init_store(
        ps.store.STORES.FILE, 'files', store_dir=STORE_DIR
    )
    key = 'key'
    store.set([1, 2, 3], key=key)

    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}

    f = FileFactory(key, 'files', STORE_DIR)
    assert f() == [1, 2, 3]

    f2 = FileFactory(key, 'files', STORE_DIR, evict=True)
    assert store.exists(key)
    assert f2() == [1, 2, 3]
    assert not store.exists(key)

    store.set([1, 2, 3], key=key)
    # Clear store to see if factory can reinitialize it
    ps.store._stores = {}
    f = FileFactory(key, 'files', STORE_DIR)
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

    store.cleanup()


def test_file_store_proxy() -> None:
    """Test FileStore Proxying"""
    store = ps.store.init_store(
        ps.store.STORES.FILE, 'files', store_dir=STORE_DIR
    )

    p = store.proxy([1, 2, 3])
    assert isinstance(p, ps.proxy.Proxy)

    assert p == [1, 2, 3]
    assert store.get(ps.proxy.get_key(p)) == [1, 2, 3]

    p2 = store.proxy(key=ps.proxy.get_key(p))
    assert p2 == [1, 2, 3]

    store.proxy([2, 3, 4], key='key')
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

    store.cleanup()


def test_proxy_recreates_store() -> None:
    """Test FileStore Proxy with FileFactory can Recreate the Store"""
    store = ps.store.init_store(
        'file', 'files', store_dir=STORE_DIR, cache_size=0
    )

    p = store.proxy([1, 2, 3], key='recreate_key')

    # Force delete store so proxy recreates it when resolved
    ps.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]

    # The store that created the proxy had cache_size=0 so the restored
    # store should also have cache_size=0.
    assert not ps.store.get_store('files').is_cached('recreate_key')

    # Repeat above but with cache_size=1
    store = ps.store.init_store(
        'file', 'files', store_dir=STORE_DIR, cache_size=1
    )
    p = store.proxy([1, 2, 3], key='recreate_key')
    ps.store._stores = {}
    assert p == [1, 2, 3]
    assert ps.store.get_store('files').is_cached('recreate_key')

    store.cleanup()

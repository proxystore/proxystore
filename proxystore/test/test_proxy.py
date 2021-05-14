"""Proxy Unit Tests"""
import numpy as np
import pickle as pkl
import subprocess
import time

from pytest import fixture, raises

import proxystore as ps
import proxystore.backend.store as store
from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy

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


def test_proxy() -> None:
    """Test Proxy behavior"""
    with raises(TypeError):
        # Proxy requires type BaseFactory
        Proxy(lambda: 'fake object')

    x = np.array([1, 2, 3])
    f = SimpleFactory(x)
    p = Proxy(f)

    assert not ps.utils.is_resolved(p)
    # BaseFactory does not use a key like KeyFactory or RedisFactory
    assert ps.utils.get_key(p) is None

    # Test pickleable
    p_pkl = pkl.dumps(p)
    p = pkl.loads(p_pkl)

    assert not ps.utils.is_resolved(p)

    # Test async
    ps.utils.resolve_async(p)
    assert p[0] == 1
    # Now async resolve should be a no-op
    ps.utils.resolve_async(p)
    assert p[1] == 2

    assert isinstance(p, Proxy)
    assert isinstance(p, np.ndarray)
    assert ps.utils.is_resolved(p)

    # Test extracting
    x_ = ps.utils.extract(p)
    assert isinstance(x_, np.ndarray)
    assert not isinstance(x_, Proxy)
    assert np.array_equal(x, x_)

    p = p + 1
    assert not isinstance(p, Proxy)
    assert np.array_equal(p, [2, 3, 4])
    assert len(p) == 3
    assert np.sum(p) == 9

    # Adding two proxies returns type of wrapped
    p = Proxy(f)
    p = p + p
    assert np.sum(p) == 12
    assert isinstance(p, np.ndarray)
    assert not isinstance(p, Proxy)

    def double(y):
        return 2 * y

    p = Proxy(f)
    res = double(p)
    assert not isinstance(res, Proxy)
    assert np.array_equal(res, [2, 4, 6])

    # TODO(gpauloski): is this expected? (see issue #1)
    p = Proxy(SimpleFactory([np.array([1, 2, 3]), np.array([2, 3, 4])]))
    res = np.sum(p, axis=0)
    assert not isinstance(res, Proxy)
    assert np.array_equal(res, [3, 5, 7])

    p = Proxy(f)
    assert isinstance(p, np.ndarray)


def test_to_proxy() -> None:
    """Test to_proxy()"""
    ps.store = None
    ps.init_local_backend()

    x = np.array([1, 2, 3])
    p = ps.to_proxy(x, key='key')
    assert 'key' == ps.utils.get_key(p)
    assert isinstance(p, Proxy)
    assert isinstance(p, np.ndarray)
    assert ps.store.exists('key')
    assert np.array_equal(p, [1, 2, 3])
    assert np.array_equal(ps.store.get('key'), [1, 2, 3])

    ps.store = None
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)

    x = np.array([1, 2, 3])
    p = ps.to_proxy(x)
    key = ps.utils.get_key(p)
    assert not ps.store.is_cached(key)
    assert isinstance(p, Proxy)
    assert isinstance(p, np.ndarray)
    assert ps.store.exists(key)
    assert ps.store.is_cached(key)
    assert np.array_equal(p, [1, 2, 3])
    assert np.array_equal(ps.store.get(key), [1, 2, 3])


def test_to_proxy_error_handling() -> None:
    """Test to_proxy() error handling"""
    ps.store = None
    with raises(RuntimeError):
        # Raises backend not initialized
        ps.to_proxy('object', 'key')

    ps.store = store.Store()
    with raises(TypeError):
        # Raises Store is an abstract class
        ps.to_proxy('object', 'key')

    ps.store = store.RemoteStore()
    with raises(TypeError):
        # Raises RemoteStore is an abstract class
        ps.to_proxy('object', 'key')

    ps.store = 'random object'
    with raises(TypeError):
        # Raises unknown backend
        ps.to_proxy('object', 'key')


def test_utils() -> None:
    """Test utils"""
    ps.store = None
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)

    x = np.array([1, 2, 3])
    p = ps.to_proxy(x, key='mykey')

    assert not ps.utils.is_resolved(p)
    ps.utils.resolve(p)
    assert ps.utils.is_resolved(p)

    ps.utils.evict(p)
    # Note: the value can still be in a local cache
    assert not ps.store.exists('mykey')

    p = ps.to_proxy(x)
    assert not ps.utils.is_resolved(p)
    # Evict will force resolve before eviction
    ps.utils.evict(p)
    assert ps.utils.is_resolved(p)

    p = Proxy(SimpleFactory(x))
    # BaseFactory does not use the store but evict should not
    # raise any errors
    ps.utils.evict(p)

    ps.store = None
    with raises(RuntimeError):
        # Raise backend not initialized error
        ps.utils.evict(p)

"""Proxy Unit Tests"""
import numpy as np
import pickle as pkl
import subprocess

from pytest import fixture, raises

import proxystore as ps
import proxystore.backend.store as store
from proxystore import backend
from proxystore.factory import BaseFactory
from proxystore.proxy import Proxy, to_proxy

REDIS_HOST = 'localhost'
REDIS_PORT = 59465


@fixture(scope='session', autouse=True)
def init() -> None:
    """Launch Redis Server for Tests"""
    redis_handle = subprocess.Popen(
        ['redis-server', '--port', str(REDIS_PORT)], stdout=subprocess.DEVNULL
    )
    yield
    redis_handle.kill()


def test_proxy() -> None:
    """Test Proxy behavior"""
    ps.store = None
    backend.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)

    with raises(TypeError):
        # Proxy requires type BaseFactory
        Proxy(lambda: 'fake object')

    x = np.array([1, 2, 3])
    f = BaseFactory(x)
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
    assert np.array_equal(p, [2, 3, 4])
    assert len(p) == 3
    assert np.sum(p) == 9

    # Adding two proxies returns type of wrapped
    p = p + p
    assert np.sum(p) == 18
    assert isinstance(p, np.ndarray)
    assert not isinstance(p, Proxy)

    p = Proxy(f)
    assert isinstance(p, np.ndarray)


def test_to_proxy() -> None:
    """Test to_proxy()"""
    ps.store = None
    backend.init_local_backend()

    x = np.array([1, 2, 3])
    p = to_proxy(x, key='key')
    assert 'key' == ps.utils.get_key(p)
    assert isinstance(p, Proxy)
    assert isinstance(p, np.ndarray)
    assert ps.store.exists('key')
    assert np.array_equal(p, [1, 2, 3])
    assert np.array_equal(ps.store.get('key'), [1, 2, 3])

    ps.store = None
    backend.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)

    x = np.array([1, 2, 3])
    p = to_proxy(x)
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
    with raises(ValueError):
        # Raises backend not initialized
        to_proxy('object', 'key')

    ps.store = store.BaseStore()
    with raises(TypeError):
        # Raises BaseStore is an abstract class
        to_proxy('object', 'key')

    ps.store = store.CachedStore()
    with raises(TypeError):
        # Raises CachedStore is an abstract class
        to_proxy('object', 'key')

    ps.store = 'random object'
    with raises(TypeError):
        # Raises unknown backend
        to_proxy('object', 'key')

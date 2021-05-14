"""Factory Unit Tests"""
import pickle as pkl
import subprocess
import time

from pytest import fixture

import proxystore as ps
from proxystore.factory import BaseFactory, KeyFactory, RedisFactory

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


def test_base_factory() -> None:
    """Test BaseFactory"""
    x = [1, 2, 3]
    f = BaseFactory(x)

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = pkl.dumps(f)
    f = pkl.loads(f_pkl)
    assert f() == [1, 2, 3]

    # Test resolve
    assert f.resolve() == [1, 2, 3]

    # async_resolve should be a no-op
    f.resolve_async()
    assert f.resolve() == [1, 2, 3]


def test_key_factory() -> None:
    """Test KeyFactory"""
    ps.store = None
    ps.init_local_backend()

    x = [1, 2, 3]
    ps.store.set('key', x)
    f = KeyFactory('key')

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = pkl.dumps(f)
    f = pkl.loads(f_pkl)
    assert f() == [1, 2, 3]

    # Test resolve
    assert f() == [1, 2, 3]

    # async_resolve should be a no-op
    f.resolve_async()
    assert f() == [1, 2, 3]


def test_redis_factory() -> None:
    """Test RedisFactory"""
    ps.store = None
    ps.backend.store._cache.reset()
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)

    x = [1, 2, 3]
    ps.store.set('key', x)
    f = RedisFactory('key', hostname=REDIS_HOST, port=REDIS_PORT)

    # Test pickleable
    f_pkl = pkl.dumps(f)
    f = pkl.loads(f_pkl)

    # Test async resolving (value should not be cached)
    assert f.obj_future is None
    f.resolve_async()
    assert f.obj_future is not None
    assert f() == [1, 2, 3]
    assert f.obj_future is None

    # Test again now that value is cached. resolve_async()
    # should just return and have no side-effects (i.e., no process spawned)
    f.resolve_async()
    assert f.obj_future is None
    assert f() == [1, 2, 3]

    # Test if Factory can initialize backend on its own
    ps.store = None
    assert f() == [1, 2, 3]
    ps.store = None
    f.resolve_async()
    assert f() == [1, 2, 3]

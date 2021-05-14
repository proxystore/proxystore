"""Factory Unit Tests"""
import pickle as pkl
import subprocess
import time

from pytest import fixture, raises

import proxystore as ps
from proxystore.factory import LambdaFactory, LocalFactory, RedisFactory

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


def test_local_factory() -> None:
    """Test LocalFactory"""
    ps.store = None

    x = [1, 2, 3]
    # Will initialize local backend as well
    f = LocalFactory(x, key='key')

    # Test x was put in store
    assert ps.store.get('key') == [1, 2, 3]

    # Test callable
    assert f() == [1, 2, 3]

    # Test just passing key
    f2 = LocalFactory(None, key='key')
    assert f2() == [1, 2, 3]

    # Test pickleable
    f_pkl = pkl.dumps(f)
    f = pkl.loads(f_pkl)
    assert f() == [1, 2, 3]

    # Test resolve
    assert f() == [1, 2, 3]

    # async_resolve should be a no-op
    f.resolve_async()
    assert f() == [1, 2, 3]

    # Test key gen
    f = LocalFactory(x)
    assert ps.store.exists(f.key)

    # Test raises
    with raises(ValueError):
        f = LocalFactory()

    # Test eviction
    f = LocalFactory([1, 2, 3, 4], key='key2', evict=True)
    assert ps.store.exists('key2')
    assert f() == [1, 2, 3, 4]
    assert not ps.store.exists('key2')


def test_redis_factory() -> None:
    """Test RedisFactory"""
    ps.store = None
    ps.backend.store._cache.reset()

    x = [1, 2, 3]
    # Will initialize RedisBackend as well
    f = RedisFactory(x, key='key', hostname=REDIS_HOST, port=REDIS_PORT)

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

    # Test just passing key since object associated with key is already
    # in store
    f2 = RedisFactory(None, key='key')
    assert f2() == [1, 2, 3]

    # Test if Factory can initialize backend on its own
    ps.store = None
    assert f() == [1, 2, 3]
    ps.store = None
    f.resolve_async()
    assert f() == [1, 2, 3]

    # Test raises because neither obj or key are provided
    with raises(ValueError):
        f = RedisFactory()

    # Test raises because Redis backend cannot be created
    ps.store = None
    with raises(ValueError):
        f = RedisFactory([1, 2, 3])

    # Test resolving hostname/port from backend
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
    f = RedisFactory([1, 2, 3])
    assert f() == [1, 2, 3]

    # Test eviction
    f = RedisFactory([1, 2, 3, 4], key='key2', evict=True)
    assert ps.store.exists('key2')
    assert f() == [1, 2, 3, 4]
    assert not ps.store.exists('key2')


def test_lambda_factory() -> None:
    """Test LambdaFactory"""
    f = LambdaFactory(lambda: [1, 2, 3])

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_pkl)
    assert f() == [1, 2, 3]

    # Test async resolve
    f.resolve_async()
    assert f() == [1, 2, 3]

    # Test with function
    def myfunc() -> str:
        return 'abc'

    f = LambdaFactory(myfunc)
    f_pkl = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_pkl)
    assert f() == 'abc'

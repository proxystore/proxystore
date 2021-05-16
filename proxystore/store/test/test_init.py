"""Store Imports and Initialization Unit Tests"""
import subprocess
import time

from pytest import fixture, raises

import proxystore as ps

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


def test_imports() -> None:
    """Test imports"""
    import proxystore.store as store

    assert callable(store.get_store)
    assert callable(store.init_store)

    from proxystore.store import get_store, init_store

    assert callable(get_store)
    assert callable(init_store)

    from proxystore.store.local import LocalStore

    LocalStore()
    ps.store.local.LocalStore()

    assert callable(ps.store.init_store)


def test_init_store() -> None:
    """Test init_store"""
    local = ps.store.init_store('local')
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store('redis', REDIS_HOST, REDIS_PORT)
    assert isinstance(redis, ps.store.redis.RedisStore)

    # Should overwrite old store
    local2 = ps.store.init_store('local')
    assert local is not local2

    with raises(ValueError):
        # Raise error for unknown name
        ps.store.init_store('unknown')


def test_get_store() -> None:
    """Test init_redis_backend"""
    local = ps.store.init_store('local')
    redis = ps.store.init_store('redis', REDIS_HOST, REDIS_PORT)
    assert local == ps.store.get_store('local')
    assert redis == ps.store.get_store('redis')

    assert ps.store.get_store('unknown') is None

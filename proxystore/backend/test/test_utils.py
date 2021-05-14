"""Backend Initialization Tests"""
import subprocess
import time

from pytest import fixture, raises

import proxystore as ps
from proxystore.backend.store import Store, RedisStore

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


def test_init_local_backend() -> None:
    """Test init_local_backend"""
    ps.store = None
    ps.init_local_backend()
    assert ps.store is not None
    assert isinstance(ps.store, Store)
    store = ps.store

    # Calling init again should do nothing since we already
    # have a Redis backend initialized
    ps.init_local_backend()
    assert store is ps.store

    ps.store = RedisStore(hostname=REDIS_PORT, port=REDIS_PORT)

    # Should raise error that a different backend is already used
    with raises(ValueError):
        ps.init_local_backend()


def test_init_redis_backend() -> None:
    """Test init_redis_backend"""
    ps.store = None
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
    assert ps.store is not None
    assert isinstance(ps.store, Store)
    assert isinstance(ps.store, RedisStore)
    store = ps.store

    # Calling init again should do nothing since we already
    # have a Redis backend initialized
    ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)
    assert store is ps.store

    ps.store = Store()

    # Should raise error that a different backend is already used
    with raises(ValueError):
        ps.init_redis_backend(hostname=REDIS_HOST, port=REDIS_PORT)

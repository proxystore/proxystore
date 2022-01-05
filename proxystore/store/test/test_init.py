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

    LocalStore(name='local')
    ps.store.local.LocalStore(name='local')

    assert callable(ps.store.init_store)


def test_init_store() -> None:
    """Test init_store/get_store"""
    from proxystore.store import STORES

    # Init by str name
    local = ps.store.init_store('local', name='local')
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store(
        'redis', name='redis', hostname=REDIS_HOST, port=REDIS_PORT
    )
    assert isinstance(redis, ps.store.redis.RedisStore)

    assert local == ps.store.get_store('local')
    assert redis == ps.store.get_store('redis')

    ps.store._stores = {}

    # Init by enum
    local = ps.store.init_store(STORES.LOCAL, name='local')
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store(
        STORES.REDIS, name='redis', hostname=REDIS_HOST, port=REDIS_PORT
    )
    assert isinstance(redis, ps.store.redis.RedisStore)

    assert local == ps.store.get_store('local')
    assert redis == ps.store.get_store('redis')

    # Init by class type
    local = ps.store.init_store(ps.store.local.LocalStore, name='local')
    assert isinstance(local, ps.store.local.LocalStore)

    ps.store._stores = {}

    # Specify name to have multiple stores of same type
    local1 = ps.store.init_store(STORES.LOCAL, 'local1')
    ps.store.init_store(STORES.LOCAL, 'local2')

    assert ps.store.get_store('local1') is not ps.store.get_store('local2')

    # Should overwrite old store
    ps.store.init_store(STORES.LOCAL, 'local1')
    assert local1 is not ps.store.get_store('local1')

    # Return None if store with name does not exist
    assert ps.store.get_store('unknown') is None


def test_init_store_raises() -> None:
    """Test init_store raises"""
    with raises(ValueError):
        # Raise error because name cannot be found in STORES
        ps.store.init_store('unknown', name='')

    with raises(ValueError):
        # Raises error because type is not a subclass of Store
        class TestStore:
            pass

        ps.store.init_store(TestStore, name='')

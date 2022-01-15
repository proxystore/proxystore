"""Store Imports and Initialization Unit Tests."""
from pytest import fixture
from pytest import raises

import proxystore as ps
from proxystore.store import STORES
from proxystore.test.store.utils import mock_third_party_libs

REDIS_HOST = "localhost"
REDIS_PORT = 59465


@fixture(scope="session", autouse=True)
def init() -> None:
    """Set up test environment."""
    mpatch = mock_third_party_libs()
    yield mpatch
    mpatch.undo()


def test_imports() -> None:
    """Test imports."""
    import proxystore.store as store

    assert callable(store.get_store)
    assert callable(store.init_store)

    from proxystore.store import get_store, init_store

    assert callable(get_store)
    assert callable(init_store)

    from proxystore.store.local import LocalStore

    LocalStore(name="local")
    ps.store.local.LocalStore(name="local")

    assert callable(ps.store.init_store)


def test_init_store() -> None:
    """Test init_store/get_store."""
    # Init by str name
    local = ps.store.init_store("local", name="local")
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store(
        "redis",
        name="redis",
        hostname=REDIS_HOST,
        port=REDIS_PORT,
    )
    assert isinstance(redis, ps.store.redis.RedisStore)

    assert local == ps.store.get_store("local")
    assert redis == ps.store.get_store("redis")

    ps.store._stores = {}

    # Init by enum
    local = ps.store.init_store(STORES.LOCAL, name="local")
    assert isinstance(local, ps.store.local.LocalStore)
    redis = ps.store.init_store(
        STORES.REDIS,
        name="redis",
        hostname=REDIS_HOST,
        port=REDIS_PORT,
    )
    assert isinstance(redis, ps.store.redis.RedisStore)

    assert local == ps.store.get_store("local")
    assert redis == ps.store.get_store("redis")

    # Init by class type
    local = ps.store.init_store(ps.store.local.LocalStore, name="local")
    assert isinstance(local, ps.store.local.LocalStore)

    ps.store._stores = {}

    # Specify name to have multiple stores of same type
    local1 = ps.store.init_store(STORES.LOCAL, "local1")
    ps.store.init_store(STORES.LOCAL, "local2")

    assert ps.store.get_store("local1") is not ps.store.get_store("local2")

    # Should overwrite old store
    ps.store.init_store(STORES.LOCAL, "local1")
    assert local1 is not ps.store.get_store("local1")

    # Return None if store with name does not exist
    assert ps.store.get_store("unknown") is None


def test_get_enum_by_type() -> None:
    """Test getting enum with type."""
    t = STORES.get_str_by_type(ps.store.local.LocalStore)
    assert isinstance(t, str)
    assert STORES[t].value == ps.store.local.LocalStore

    class FakeStore(ps.store.base.Store):
        """FakeStore type."""

        pass

    with raises(KeyError):
        STORES.get_str_by_type(FakeStore)


def test_init_store_raises() -> None:
    """Test init_store raises."""
    with raises(ValueError):
        # Raise error because name cannot be found in STORES
        ps.store.init_store("unknown", name="")

    with raises(ValueError):
        # Raises error because type is not a subclass of Store
        class TestStore:
            pass

        ps.store.init_store(TestStore, name="")

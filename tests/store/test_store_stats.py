"""RemoteStore Stat Tracking Tests."""
import os
import shutil

from pytest import fixture
from pytest import mark
from pytest import raises

import proxystore as ps
from .utils import FILE_DIR
from .utils import FILE_STORE
from .utils import GLOBUS_STORE
from .utils import LOCAL_STORE
from .utils import mock_third_party_libs
from .utils import REDIS_STORE


@fixture(scope="session", autouse=True)
def init():
    """Set up test environment."""
    mpatch = mock_third_party_libs()
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)  # pragma: no cover
    yield mpatch
    mpatch.undo()
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)  # pragma: no cover


@mark.parametrize(
    "store_config",
    [LOCAL_STORE, FILE_STORE, REDIS_STORE, GLOBUS_STORE],
)
def test_init_stats(store_config) -> None:
    """Test Initializing Stat tracking."""
    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
    )

    with raises(ValueError):
        # Check raises an error because stats are not tracked by default
        store.stats("key")

    store = store_config["type"](
        store_config["name"],
        **store_config["kwargs"],
        stats=True,
    )

    assert isinstance(store.stats("key"), dict)


def test_stat_tracking() -> None:
    """Test stat tracking of store."""
    store = ps.store.init_store("local", "local", stats=True)

    p = store.proxy([1, 2, 3], key="key")
    ps.proxy.resolve(p)

    stats = store.stats("key")

    assert "get" in stats
    assert "set" in stats

    assert stats["get"].calls == 1
    assert stats["set"].calls == 1

    # stats should return a copy of the stats, not the internal data
    # structures so calling get again should not effect anything.
    store.get("key")

    assert stats["get"].calls == 1

    stats = store.stats("missing_key")

    assert len(stats) == 0


@mark.parametrize(
    "store_config",
    [FILE_STORE, REDIS_STORE, GLOBUS_STORE],
)
def test_factory_preserves_tracking(store_config) -> None:
    """Test Factories Preserve the Stat Tracking Flag."""
    store = ps.store.init_store(
        store_config["type"],
        store_config["name"],
        **store_config["kwargs"],
        stats=True,
    )

    p = store.proxy([1, 2, 3])
    key = ps.proxy.get_key(p)

    # Force delete store so proxy recreates it when resolved
    ps.store._stores = {}

    # Resolve the proxy
    assert p == [1, 2, 3]
    store = ps.store.get_store(store_config["name"])

    assert isinstance(store.stats(key), dict)
    assert store.stats(key)["get"].calls == 1

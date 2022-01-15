"""RedisStore Unit Tests."""
from pytest import fixture

from proxystore.store.redis import RedisStore
from proxystore.test.store.utils import mock_third_party_libs
from proxystore.test.store.utils import REDIS_STORE


@fixture(scope="session", autouse=True)
def init() -> None:
    """Set up test environment."""
    mpatch = mock_third_party_libs()
    yield mpatch
    mpatch.undo()


def test_kwargs() -> None:
    """Test RedisFactory kwargs."""
    store = RedisStore(REDIS_STORE["name"], **REDIS_STORE["kwargs"])
    assert store.kwargs == {
        **REDIS_STORE["kwargs"],
        "cache_size": store.cache_size,
    }
    store.cleanup()

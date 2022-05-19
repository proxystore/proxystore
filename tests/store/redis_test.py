"""RedisStore Unit Tests."""
from __future__ import annotations

from pytest import fixture

from proxystore.store.redis import RedisStore
from testing.store_utils import mock_third_party_libs
from testing.store_utils import REDIS_STORE


@fixture(scope='session', autouse=True)
def init():
    """Set up test environment."""
    mpatch = mock_third_party_libs()
    yield mpatch
    mpatch.undo()


def test_kwargs() -> None:
    """Test RedisFactory kwargs."""
    store = RedisStore(REDIS_STORE['name'], **REDIS_STORE['kwargs'])
    for key, value in REDIS_STORE['kwargs'].items():
        assert store.kwargs[key] == value

    assert store._kwargs({'test': 1})['test'] == 1
    store.close()

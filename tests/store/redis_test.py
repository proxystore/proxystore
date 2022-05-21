"""RedisStore Unit Tests."""
from __future__ import annotations

from proxystore.store.redis import RedisStore


def test_kwargs(redis_store) -> None:
    """Test RedisStore kwargs."""
    store = RedisStore(redis_store.name, **redis_store.kwargs)
    for key, value in redis_store.kwargs.items():
        assert store.kwargs[key] == value

    assert store._kwargs({'test': 1})['test'] == 1
    store.close()

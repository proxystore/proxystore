"""RedisStore Unit Tests."""
from __future__ import annotations

from proxystore.store.redis import RedisStore


def test_redis_store(redis_store) -> None:
    """Test RedisStore.

    All RedisStore functionality should be covered in
    tests/store/store_*_test.py.
    """
    RedisStore(redis_store.name, **redis_store.kwargs)

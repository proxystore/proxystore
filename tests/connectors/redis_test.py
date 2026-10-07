from __future__ import annotations

import pytest
import redis

from proxystore.connectors.redis import RedisConnector
from testing.mocked.redis import MockStrictRedis


# Use redis_connector because it mocks StrictRedis client to act
# like there is a single shared Redis server.
def test_close_persists_keys_by_default(redis_connector) -> None:
    connector = RedisConnector('localhost', 0)
    key = connector.put(b'value')

    assert connector.exists(key)
    assert connector.close() is False
    # This only works with the mocked connector because otherwise
    # the connection pool used by Redis would have been closed
    assert connector.exists(key)


def test_close_override_default(redis_connector) -> None:
    connector = RedisConnector('localhost', 0, clear=False)
    key = connector.put(b'value')

    assert connector.exists(key)
    assert connector.close(clear=True) is True
    assert not connector.exists(key)


def test_multiple_closed_connectors(redis_connector) -> None:
    connector1 = RedisConnector('localhost', 0)
    connector2 = RedisConnector('localhost', 0)
    key = connector1.put(b'value')

    assert connector1.exists(key)
    connector1.close(clear=True)
    connector2.close(clear=True)
    assert not connector2.exists(key)


def test_empty_batch(redis_connector) -> None:
    connector = RedisConnector('localhost', 0)
    assert connector.put_batch([]) == []
    assert connector.get_batch([]) == []
    connector.close()


def test_mocked_redis_rejects_empty_batch() -> None:
    # The mocked client should reject empty batches like a Redis server.
    client = MockStrictRedis({})
    with pytest.raises(redis.exceptions.ResponseError, match='mget'):
        client.mget([])
    with pytest.raises(redis.exceptions.ResponseError, match='mset'):
        client.mset({})

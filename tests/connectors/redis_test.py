from __future__ import annotations

from proxystore.connectors.redis import RedisConnector


# Use redis_connector because it mocks StrictRedis client to act
# like there is a single shared Redis server.
def test_close_persists_keys_by_default(redis_connector) -> None:
    connector = RedisConnector('localhost', 0)
    key = connector.put(b'value')

    assert connector.exists(key)
    connector.close()
    # This only works with the mocked connector because otherwise
    # the connection pool used by Redis would have been closed
    assert connector.exists(key)


def test_close_override_default(redis_connector) -> None:
    connector = RedisConnector('localhost', 0, clear=False)
    key = connector.put(b'value')

    assert connector.exists(key)
    connector.close(clear=True)
    assert not connector.exists(key)


def test_multiple_closed_connectors(redis_connector) -> None:
    connector1 = RedisConnector('localhost', 0)
    connector2 = RedisConnector('localhost', 0)
    key = connector1.put(b'value')

    assert connector1.exists(key)
    connector1.close(clear=True)
    connector2.close(clear=True)
    assert not connector2.exists(key)

from __future__ import annotations

import queue
from typing import Any
from typing import Generator
from unittest import mock

import pytest

from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber
from testing.mocked.redis import Message
from testing.mocked.redis import MockStrictRedis


@pytest.fixture(autouse=True)
def _mock_redis() -> Generator[None, None, None]:
    redis_store: dict[str, bytes] = {}
    redis_queue: queue.Queue[Message] = queue.Queue()

    def create_mocked_redis(*args: Any, **kwargs: Any) -> MockStrictRedis:
        return MockStrictRedis(redis_store, redis_queue, *args, **kwargs)

    with mock.patch('redis.StrictRedis', side_effect=create_mocked_redis):
        yield


def test_basic_publish_subscribe() -> None:
    publisher = RedisPublisher('localhost', 0)
    subscriber = RedisSubscriber('localhost', 0, 'default')

    messages = [f'message_{i}'.encode() for i in range(3)]

    for message in messages:
        publisher.send('default', message)

    publisher.close()

    received = []
    for _, message in zip(messages, subscriber):
        received.append(message)

    subscriber.close()

    assert messages == received

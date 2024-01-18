"""Mocked classes for Redis."""
from __future__ import annotations

import queue
from typing import Any
from typing import TypedDict


class Message(TypedDict):
    """Pub/sub message type."""

    pattern: bytes | None
    type: str
    channel: bytes
    data: Any


class MockStrictRedis:
    """Mock StrictRedis."""

    def __init__(
        self,
        data: dict[str, Any],
        pubsub_queue: queue.Queue[Message] | None = None,
        *args,
        **kwargs,
    ):
        self.data = data
        self.pubsub_queue = (
            queue.Queue() if pubsub_queue is None else pubsub_queue
        )

    def close(self) -> None:
        """Close the client."""
        pass

    def delete(self, key: str) -> None:
        """Delete key."""
        if key in self.data:
            del self.data[key]

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data

    def flushdb(self) -> None:
        """Remove all keys."""
        self.data.clear()

    def get(self, key: str) -> bytes | None:
        """Get value with key."""
        if key in self.data:
            return self.data[key]
        return None

    def mget(self, keys: list[str]) -> list[bytes | None]:
        """Get list of values from keys."""
        return [self.data.get(key, None) for key in keys]

    def mset(self, values: dict[str, bytes]) -> None:
        """Set list of values."""
        for key, value in values.items():
            self.set(key, value)

    def publish(self, topic: str, data: bytes) -> None:
        """Publish a message to a topic."""
        message = Message(
            pattern=None,
            type='message',
            channel=topic.encode(),
            data=data,
        )
        self.pubsub_queue.put(message)

    def pubsub(self) -> MockPubSub:
        """Create a pubsub client."""
        return MockPubSub(self)

    def set(self, key: str, value: bytes) -> None:
        """Set value in MockStrictRedis."""
        self.data[key] = value


class MockPubSub:
    """Mock PubSub client."""

    def __init__(self, redis: MockStrictRedis):
        self.redis = redis
        self.subscribed: set[str] = set()

    def close(self) -> None:
        """Close the pub/sub client."""
        pass

    def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float | None = None,
    ) -> Message | None:
        """Get the next message from subscribed topics."""
        if len(self.subscribed) == 0:  # pragma: no cover
            raise RuntimeError('Not subscribed to any topics.')

        while True:
            message = self.redis.pubsub_queue.get(timeout=timeout)
            if (
                message['channel'].decode() not in self.subscribed
            ):  # pragma: no cover
                continue
            if message['type'] == 'subscribe' and ignore_subscribe_messages:
                return None
            else:
                return message

    def subscribe(self, *topics: str) -> None:
        """Subscribe to a topic."""
        for topic in topics:
            self.subscribed.add(topic)
            message = Message(
                pattern=None,
                type='subscribe',
                channel=topic.encode(),
                data=len(self.subscribed),
            )
            self.redis.pubsub_queue.put(message)

    def unsubscribe(self) -> None:
        """Unsubscribe from all topics."""
        for i, topic in enumerate(self.subscribed):
            message = Message(
                pattern=None,
                type='unsubscribe',
                channel=topic.encode(),
                data=len(self.subscribed) - i,
            )
            self.redis.pubsub_queue.put(message)
        self.subscribed = set()

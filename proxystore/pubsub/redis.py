"""Redis pub/sub interface."""
from __future__ import annotations

import sys
from typing import Any
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import redis

_CLOSED_SENTINAL = b'<queue-publisher-closed-topic>'


class RedisPublisher:
    """Publisher interface to Redis pub/sub.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
        topics: Sequence or set of all topics that might be published to.
        default_topic: Default topic to publish messages to. Must be contained
            in `topics`.
        kwargs: Extra keyword arguments to pass to
            [`redis.Redis()`][redis.Redis].

    Raises:
        ValueError: if `default_topic` is not in `topics`.
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        *,
        topics: Sequence[str] | set[str] = ('default',),
        default_topic: str = 'default',
        **kwargs: Any,
    ) -> None:
        if default_topic not in topics:
            raise ValueError(
                f'Default topic "{default_topic}" is not in the list of '
                f'all topic: {topics}.',
            )
        self._topics = topics
        self._default_topic = default_topic
        self._redis_client = redis.StrictRedis(
            host=hostname,
            port=port,
            **kwargs,
        )

    def close(self, *, close_topics: bool = True) -> None:
        """Close this publisher.

        This will cause a [`StopIteration`][StopIteration] exception to be
        raised in any
        [`RedisSubscriber`][proxystore.pubsub.redis.RedisSubscriber]
        instances that are currently iterating on new messages from *any*
        of the topics registered with this publisher. This behavior
        can be altered by passing `close_topics=True`.

        Args:
            close_topics: Send an end-of-stream message to all topics
                associated with this publisher.
        """
        if close_topics:
            for topic in self._topics:
                self._redis_client.publish(topic, _CLOSED_SENTINAL)
        self._redis_client.close()

    def send(self, message: bytes, *, topic: str | None = None) -> None:
        """Publish a message to the stream.

        Args:
            message: Message as bytes to publish to the stream.
            topic: Stream topic to publish to. `None` uses the default stream.

        Raises:
            ValueError: if `topic` is not in `topics` provided during
                initialization.
        """
        topic = topic if topic is not None else self._default_topic
        if topic not in self._topics:
            raise ValueError(f'Topic "{topic}" is unknown.')
        self._redis_client.publish(topic, message)


class RedisSubscriber:
    """Subscriber interface to Redis pub/sub topic.

    The subscriber protocol is an iterable object which yields objects
    from the stream until the stream is closed.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
        topic: Topic or sequence of topics to subscribe to.
        kwargs: Extra keyword arguments to pass to
            [`redis.Redis()`][redis.Redis].
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        *,
        topic: str | Sequence[str] = 'default',
        **kwargs: Any,
    ) -> None:
        self._topic = [topic] if isinstance(topic, str) else topic
        self._redis_client = redis.StrictRedis(
            host=hostname,
            port=port,
            **kwargs,
        )
        self._pubsub_client = self._redis_client.pubsub()
        self._pubsub_client.subscribe(*self._topic)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        while True:
            message = self._pubsub_client.get_message(
                ignore_subscribe_messages=True,
                # The type hint from redis is "timeout: float" but the
                # docstring and code also support None type.
                # https://github.com/redis/redis-py/blob/0a824962e9c0f8ec1b6b9b0fc823db8ec296e580/redis/client.py#L1046
                timeout=None,  # type: ignore[arg-type]
            )
            if message is None:
                # None is returned in a few cases, such as the message
                # beign given to a handler or when subscribe messages
                # are ignored.
                continue

            kind = message['type']
            data = message['data']

            if (
                kind in redis.client.PubSub.UNSUBSCRIBE_MESSAGE_TYPES
            ):  # pragma: no cover
                raise StopIteration
            elif kind in redis.client.PubSub.PUBLISH_MESSAGE_TYPES:
                if data == _CLOSED_SENTINAL:
                    self._pubsub_client.unsubscribe()
                    raise StopIteration
                return data
            else:  # pragma: no cover
                # This case is pings and health check messages.
                continue

    def close(self) -> None:
        """Close this subscriber."""
        self._pubsub_client.close()
        self._redis_client.close()

"""Redis publisher and subscriber shims.

Shims to the
[`redis-py`](https://redis-py.readthedocs.io/en/stable/index.html){target=_blank}
[Publish / Subscribe interface](https://redis-py.readthedocs.io/en/stable/advanced_features.html#publish-subscribe){target=_blank}.
"""
from __future__ import annotations

import sys
from typing import Any
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import redis


class RedisPublisher:
    """Redis pub/sub publisher shim.

    Args:
        hostname: Redis server hostname.
        port: Redis server port.
        kwargs: Extra keyword arguments to pass to
            [`redis.Redis()`][redis.Redis].
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        **kwargs: Any,
    ) -> None:
        self._redis_client = redis.StrictRedis(
            host=hostname,
            port=port,
            **kwargs,
        )

    def close(self) -> None:
        """Close this publisher."""
        self._redis_client.close()

    def send(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.
        """
        self._redis_client.publish(topic, message)


class RedisSubscriber:
    """Redis pub/sub subscriber shim.

    This shim is an iterable object which will yield [`bytes`][bytes]
    messages from the stream, blocking on the next message, until the stream
    is closed.

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
        topic: str | Sequence[str],
        **kwargs: Any,
    ) -> None:
        self._redis_client = redis.StrictRedis(
            host=hostname,
            port=port,
            **kwargs,
        )
        self._topics = [topic] if isinstance(topic, str) else topic
        self._pubsub_client = self._redis_client.pubsub()
        self._pubsub_client.subscribe(*self._topics)

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
                return data
            else:  # pragma: no cover
                # This case is pings and health check messages.
                continue

    def close(self) -> None:
        """Close this subscriber."""
        self._pubsub_client.unsubscribe()
        self._pubsub_client.close()
        self._redis_client.close()

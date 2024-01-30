"""Kafka pub/sub interface."""
from __future__ import annotations

import sys
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import kafka

_CLOSED_SENTINAL = b'<queue-publisher-closed-topic>'


class KafkaPublisher:
    """Publisher interface to Kafka message stream.

    Args:
        client: [`KafkaProducer`][kafka.KafkaProducer] client.
        topics: Sequence or set of all topics that might be published to.
    """

    def __init__(
        self,
        client: kafka.KafkaProducer,
        *,
        topics: Sequence[str] | set[str] = ('default',),
    ) -> None:
        self._topics = topics
        self._client = client

    def close(self, *, close_topics: bool = True) -> None:
        """Close this publisher.

        This will cause a [`StopIteration`][StopIteration] exception to be
        raised in any
        [`KafkaSubscriber`][proxystore.stream.shims.kafka.KafkaSubscriber]
        instances that are currently iterating on new messages from *any*
        of the topics registered with this publisher. This behavior can be
        altered by passing `close_topics=True`.

        Args:
            close_topics: Send an end-of-stream message to all topics
                associated with this publisher.
        """
        if close_topics:
            for topic in self._topics:
                future = self._client.send(topic, _CLOSED_SENTINAL)
                future.get()
        self._client.close()

    def send(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.

        Raises:
            ValueError: if `topic` is not in `topics` provided during
                initialization.
        """
        if topic not in self._topics:
            raise ValueError(f'Topic "{topic}" is unknown.')
        future = self._client.send(topic, message)
        future.get()


class KafkaSubscriber:
    """Subscriber interface to message stream.

    The subscriber protocol is an iterable object which yields objects
    from the stream until the stream is closed.

    Args:
        client: [`KafkaConsumer`][kafka.KafkaConsumer] client.
    """

    def __init__(self, client: kafka.KafkaConsumer) -> None:
        self._client = client

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        message = next(self._client)
        if message.value == _CLOSED_SENTINAL:
            raise StopIteration
        return message.value

    def close(self) -> None:
        """Close this subscriber."""
        self._client.close()

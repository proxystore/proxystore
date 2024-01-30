"""Kafka publisher and subscriber shims.

Shims to the
[`kafka-python`](https://github.com/dpkp/kafka-python){target=_blank} package.
"""
from __future__ import annotations

import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import kafka


class KafkaPublisher:
    """Kafka publisher shim.

    Args:
        client: [`KafkaProducer`][kafka.KafkaProducer] client.
    """

    def __init__(self, client: kafka.KafkaProducer) -> None:
        self.client = client

    def close(self) -> None:
        """Close this publisher."""
        self.client.close()

    def send(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.
        """
        future = self.client.send(topic, message)
        future.get()


class KafkaSubscriber:
    """Kafka subscriber shim.

    This shim is an iterable object which will yield [`bytes`][bytes]
    messages from the stream, blocking on the next message, until the stream
    is closed.

    Args:
        client: [`KafkaConsumer`][kafka.KafkaConsumer] client.
    """

    def __init__(self, client: kafka.KafkaConsumer) -> None:
        self.client = client

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        message = next(self.client)
        return message.value

    def close(self) -> None:
        """Close this subscriber."""
        self.client.close()

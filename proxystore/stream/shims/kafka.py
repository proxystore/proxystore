"""Kafka publisher and subscriber shims.

Shims to the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python){target=_blank}
package.
"""

from __future__ import annotations

from typing import Self

import confluent_kafka

# Seconds to wait for messages to be delivered when the local queue is full.
_FULL_QUEUE_POLL_TIMEOUT = 1


class KafkaPublisher:
    """Kafka publisher shim.

    Messages are delivered in the background, and
    [`close()`][proxystore.stream.shims.kafka.KafkaPublisher.close] waits
    for every message to be delivered.

    Args:
        client: Kafka producer client.
    """

    def __init__(self, client: confluent_kafka.Producer) -> None:
        self.client = client

    def close(self) -> None:
        """Close this publisher."""
        self.client.flush()

    def send_message(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.
        """
        # Messages are sent in the background and flushed in close().
        # Flushing after each message would wait for every message to be
        # delivered before the next is sent.
        while True:
            try:
                self.client.produce(topic, message)
            except BufferError:
                # The local queue is full, so wait for some messages to be
                # delivered and try again.
                self.client.poll(_FULL_QUEUE_POLL_TIMEOUT)
            else:
                break
        # Serve delivery callbacks of earlier messages without blocking.
        self.client.poll(0)


class KafkaSubscriber:
    """Kafka subscriber shim.

    This shim is an iterable object which will yield [`bytes`][bytes]
    messages from the stream, blocking on the next message, until the stream
    is closed.

    Args:
        client: Kafka consumer client. The `client` must already be subscribed
            to the relevant topics.
    """

    def __init__(self, client: confluent_kafka.Consumer) -> None:
        self.client = client

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        return self.next_message()

    def next_message(self) -> bytes:
        """Get the next message."""
        message = self.client.poll()
        # Should not be None because we do not specify a poll in timeout.
        assert message is not None
        if message.error() is not None:  # pragma: no cover
            raise confluent_kafka.KafkaException(message.error())
        return message.value()

    def close(self) -> None:
        """Close this subscriber."""
        self.client.close()

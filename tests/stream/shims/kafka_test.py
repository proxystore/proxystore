from __future__ import annotations

from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber
from testing.mocked.kafka import make_producer_consumer_pair


def test_basic_publish_subscribe() -> None:
    producer, consumer = make_producer_consumer_pair('default')
    publisher = KafkaPublisher(producer)
    subscriber = KafkaSubscriber(consumer)

    messages = [f'message_{i}'.encode() for i in range(3)]

    for message in messages:
        publisher.send_message('default', message)

    publisher.close()

    for expected, received in zip(messages, subscriber):
        assert received == expected

    subscriber.close()

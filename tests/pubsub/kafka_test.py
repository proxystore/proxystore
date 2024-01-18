from __future__ import annotations

import pytest

from proxystore.pubsub.kafka import KafkaPublisher
from proxystore.pubsub.kafka import KafkaSubscriber
from testing.mocked.kafka import make_producer_consumer_pair


def test_bad_publisher_default_topic() -> None:
    producer, _ = make_producer_consumer_pair()

    with pytest.raises(ValueError, match='other'):
        KafkaPublisher(producer, topics=['default'], default_topic='other')


def test_publish_unknown_topic() -> None:
    topic = 'default'
    producer, _ = make_producer_consumer_pair(topic)

    publisher = KafkaPublisher(producer, topics=[topic], default_topic=topic)

    with pytest.raises(ValueError, match='other'):
        publisher.send(b'message', topic='other')

    publisher.close()


def test_publisher_close_client_only() -> None:
    producer, _ = make_producer_consumer_pair()
    publisher = KafkaPublisher(producer)
    publisher.close(close_topics=False)


def test_basic_publish_subscribe() -> None:
    producer, consumer = make_producer_consumer_pair('default')
    publisher = KafkaPublisher(producer)
    subscriber = KafkaSubscriber(consumer)

    messages = [f'message_{i}'.encode() for i in range(3)]

    for message in messages:
        publisher.send(message)

    publisher.close()

    received = []
    for message in subscriber:
        received.append(message)

    subscriber.close()

    assert messages == received

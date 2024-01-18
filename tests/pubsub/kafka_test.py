from __future__ import annotations

import sys

import pytest

try:
    import kafka

    from proxystore.pubsub.kafka import KafkaPublisher
    from proxystore.pubsub.kafka import KafkaSubscriber
    from testing.mocked.kafka import make_producer_consumer_pair

    kafka_available = True
except ImportError:  # pragma: no cover
    kafka_available = False

if kafka_available:  # pragma: no branch
    kafka_version = tuple(kafka.__version__.split('.'))

skip_py312 = not kafka_available or (
    sys.version_info >= (3, 12) and kafka_version <= (2, 0, 2)
)
skip_py312_reason = 'kafka-python<=2.0.2 is not compatible with Python 3.12'


@pytest.mark.skipif(skip_py312, reason=skip_py312_reason)
def test_bad_publisher_default_topic() -> None:
    producer, _ = make_producer_consumer_pair()

    with pytest.raises(ValueError, match='other'):
        KafkaPublisher(producer, topics=['default'], default_topic='other')


@pytest.mark.skipif(skip_py312, reason=skip_py312_reason)
def test_publish_unknown_topic() -> None:
    topic = 'default'
    producer, _ = make_producer_consumer_pair(topic)

    publisher = KafkaPublisher(producer, topics=[topic], default_topic=topic)

    with pytest.raises(ValueError, match='other'):
        publisher.send(b'message', topic='other')

    publisher.close()


@pytest.mark.skipif(skip_py312, reason=skip_py312_reason)
def test_publisher_close_client_only() -> None:
    producer, _ = make_producer_consumer_pair()
    publisher = KafkaPublisher(producer)
    publisher.close(close_topics=False)


@pytest.mark.skipif(skip_py312, reason=skip_py312_reason)
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

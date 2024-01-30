from __future__ import annotations

import sys

import pytest

try:
    import kafka

    from proxystore.stream.shims.kafka import KafkaPublisher
    from proxystore.stream.shims.kafka import KafkaSubscriber
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
def test_basic_publish_subscribe() -> None:
    producer, consumer = make_producer_consumer_pair('default')
    publisher = KafkaPublisher(producer)
    subscriber = KafkaSubscriber(consumer)

    messages = [f'message_{i}'.encode() for i in range(3)]

    for message in messages:
        publisher.send('default', message)

    publisher.close()

    for expected, received in zip(messages, subscriber):
        assert received == expected

    subscriber.close()

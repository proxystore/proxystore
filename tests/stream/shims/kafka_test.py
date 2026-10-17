from __future__ import annotations

from unittest import mock

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

    for expected, received in zip(messages, subscriber, strict=False):
        assert received == expected

    subscriber.close()


def test_publish_does_not_flush() -> None:
    producer, _ = make_producer_consumer_pair('default')
    publisher = KafkaPublisher(producer)

    with (
        mock.patch.object(producer, 'flush') as flush,
        mock.patch.object(producer, 'poll') as poll,
    ):
        publisher.send_message('default', b'message')
        flush.assert_not_called()
        poll.assert_called_once_with(0)

        publisher.close()
        flush.assert_called_once()


def test_publish_retries_when_queue_full() -> None:
    producer, _ = make_producer_consumer_pair('default')
    publisher = KafkaPublisher(producer)

    with (
        mock.patch.object(
            producer,
            'produce',
            side_effect=[BufferError, BufferError, None],
        ) as produce,
        mock.patch.object(producer, 'poll') as poll,
    ):
        publisher.send_message('default', b'message')

    assert produce.call_count == 3
    assert poll.call_args_list == [
        mock.call(1),
        mock.call(1),
        mock.call(0),
    ]

from __future__ import annotations

import threading

import pytest

from proxystore.pubsub.zmq import ZeroMQPublisher
from proxystore.pubsub.zmq import ZeroMQSubscriber
from testing.utils import open_port


def test_bad_publisher_default_topic() -> None:
    with pytest.raises(ValueError, match='other'):
        ZeroMQPublisher(
            '127.0.0.1',
            0,
            topics=['default'],
            default_topic='other',
        )


def test_publish_unknown_topic() -> None:
    publisher = ZeroMQPublisher(
        '127.0.0.1',
        open_port(),
        topics=['default'],
        default_topic='default',
    )

    with pytest.raises(ValueError, match='other'):
        publisher.send(b'message', topic='other')

    publisher.close()


def test_publisher_close_client_only() -> None:
    publisher = ZeroMQPublisher('127.0.0.1', open_port())
    publisher.close(close_topics=False)


def publish(publisher: ZeroMQPublisher, messages: list[bytes]) -> None:
    for message in messages:
        publisher.send(message)


def subscribe(
    subscriber: ZeroMQSubscriber,
    publisher: ZeroMQPublisher,
    messages: list[bytes],
) -> None:
    received = []

    for message in subscriber:
        received.append(message)

        if len(received) == len(messages):
            publisher.close()

    assert received == messages

    subscriber.close()


def test_basic_publish_subscribe() -> None:
    address, port = '127.0.0.1', open_port()

    publisher = ZeroMQPublisher(address, port)
    subscriber = ZeroMQSubscriber(address, port)

    messages = [f'message_{i}'.encode() for i in range(3)]

    pproc = threading.Thread(target=publish, args=[publisher, messages])
    sproc = threading.Thread(
        target=subscribe,
        args=[subscriber, publisher, messages],
    )

    sproc.start()
    pproc.start()

    pproc.join(timeout=5)
    sproc.join(timeout=5)

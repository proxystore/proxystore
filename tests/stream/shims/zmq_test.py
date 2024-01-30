from __future__ import annotations

from unittest import mock

import pytest

from proxystore.stream.shims.zmq import _CLOSED_SENTINAL
from proxystore.stream.shims.zmq import ZeroMQPublisher
from proxystore.stream.shims.zmq import ZeroMQSubscriber
from testing.utils import open_port


def test_publish_unknown_topic() -> None:
    publisher = ZeroMQPublisher('127.0.0.1', open_port(), topics=['default'])

    with pytest.raises(ValueError, match='other'):
        publisher.send('other', b'message')

    publisher.close()


def test_publisher_close_client_only() -> None:
    publisher = ZeroMQPublisher('127.0.0.1', open_port())
    publisher.close(close_topics=False)


def test_basic_publish_subscribe() -> None:
    address, port = '127.0.0.1', open_port()

    publisher = ZeroMQPublisher(address, port)
    subscriber = ZeroMQSubscriber(address, port)

    messages = [f'message_{i}'.encode() for i in range(3)]

    with mock.patch.object(publisher._socket, 'send_multipart'):
        for message in messages:
            publisher.send('default', message)

        publisher.close()

    received = []

    with mock.patch.object(
        subscriber._socket,
        'recv_multipart',
        side_effect=[
            (b'default', message) for message in [*messages, _CLOSED_SENTINAL]
        ],
    ):
        for message in subscriber:
            received.append(message)

    subscriber.close()

    assert messages == received

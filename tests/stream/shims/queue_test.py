from __future__ import annotations

import multiprocessing
import queue
import threading

import pytest

from proxystore.stream.protocols import Publisher
from proxystore.stream.protocols import Subscriber
from proxystore.stream.shims.queue import QueuePublisher
from proxystore.stream.shims.queue import QueueSubscriber


def create_pubsub_pair(
    queue_: multiprocessing.Queue[bytes] | queue.Queue[bytes],
) -> tuple[QueuePublisher, QueueSubscriber]:
    topic = 'default'

    publisher = QueuePublisher({topic: queue_})
    subscriber = QueueSubscriber(queue_)

    return publisher, subscriber


def test_unknown_topic() -> None:
    publisher = QueuePublisher({'default': queue.Queue()})

    with pytest.raises(ValueError, match='Unknown topic "other".'):
        publisher.send('other', b'message')


def test_multiprocessing_implements_protocol() -> None:
    publisher, subscriber = create_pubsub_pair(multiprocessing.Queue())

    assert isinstance(publisher, Publisher)
    assert isinstance(subscriber, Subscriber)


def test_threading_implements_protocol() -> None:
    publisher, subscriber = create_pubsub_pair(queue.Queue())

    assert isinstance(publisher, Publisher)
    assert isinstance(subscriber, Subscriber)


def test_multiprocessing_open_close() -> None:
    publisher, subscriber = create_pubsub_pair(multiprocessing.Queue())

    publisher.close()

    messages = list(subscriber)
    assert len(messages) == 0

    subscriber.close()


def publish(publisher: QueuePublisher, messages: list[bytes]) -> None:
    for message in messages:
        publisher.send('default', message)
    publisher.close()


def subscribe(subscriber: QueueSubscriber, messages: list[bytes]) -> None:
    received = []

    for _, message in zip(messages, subscriber):
        received.append(message)

    assert received == messages


def test_multiprocessing_send_messages() -> None:
    publisher, subscriber = create_pubsub_pair(multiprocessing.Queue())

    messages = [b'message-{i}' for i in range(10)]

    pproc = multiprocessing.Process(target=publish, args=[publisher, messages])
    sproc = multiprocessing.Process(
        target=subscribe,
        args=[subscriber, messages],
    )

    sproc.start()
    pproc.start()

    pproc.join(timeout=5)
    sproc.join(timeout=5)

    assert pproc.exitcode == 0
    assert sproc.exitcode == 0


def test_threading_send_messages() -> None:
    publisher, subscriber = create_pubsub_pair(queue.Queue())

    messages = [b'message-{i}' for i in range(10)]

    pthread = threading.Thread(target=publish, args=[publisher, messages])
    sthread = threading.Thread(target=subscribe, args=[subscriber, messages])

    sthread.start()
    pthread.start()

    pthread.join(timeout=5)
    sthread.join(timeout=5)

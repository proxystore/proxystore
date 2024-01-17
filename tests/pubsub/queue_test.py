from __future__ import annotations

import multiprocessing
import queue
import threading

import pytest

from proxystore.pubsub.protocols import Publisher
from proxystore.pubsub.protocols import Subscriber
from proxystore.pubsub.queue import QueuePublisher
from proxystore.pubsub.queue import QueueSubscriber


def create_pubsub_pair(
    queue_: multiprocessing.Queue[bytes] | queue.Queue[bytes],
) -> tuple[QueuePublisher, QueueSubscriber]:
    topic = 'default'

    publisher = QueuePublisher({topic: queue_}, topic)
    subscriber = QueueSubscriber(queue_)

    return publisher, subscriber


def test_bad_default_topic() -> None:
    with pytest.raises(ValueError, match='Default topic'):
        QueuePublisher({'default': queue.Queue()}, 'different-default')


def test_unknown_topic() -> None:
    publisher = QueuePublisher({'default': queue.Queue()}, 'default')

    with pytest.raises(ValueError, match='Topic "other" does not exist.'):
        publisher.send(b'message', topic='other')


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


def test_threading_open_close() -> None:
    publisher, subscriber = create_pubsub_pair(queue.Queue())

    publisher.close()

    messages = list(subscriber)
    assert len(messages) == 0


def publish(publisher: QueuePublisher, messages: list[bytes]) -> None:
    for message in messages:
        publisher.send(message)
    publisher.close()


def subscribe(subscriber: QueueSubscriber, messages: list[bytes]) -> None:
    received = []

    for message in subscriber:
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

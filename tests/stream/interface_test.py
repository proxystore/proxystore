from __future__ import annotations

import queue
import threading
import uuid
from typing import Generator

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.store import store_registration
from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer
from proxystore.stream.shims.queue import QueuePublisher
from proxystore.stream.shims.queue import QueueSubscriber


def create_pubsub_pair() -> tuple[QueuePublisher, QueueSubscriber]:
    topic = 'default'
    queue_: queue.Queue[bytes] = queue.Queue()

    publisher = QueuePublisher({topic: queue_})
    subscriber = QueueSubscriber(queue_)

    return publisher, subscriber


@pytest.fixture()
def store() -> Generator[Store[LocalConnector], None, None]:
    with Store('stream-test-fixture', LocalConnector()) as store:
        with store_registration(store):
            yield store


def test_basic_interface(store: Store[LocalConnector]) -> None:
    publisher, subscriber = create_pubsub_pair()

    producer = StreamProducer(store, publisher)
    consumer = StreamConsumer(store, subscriber)

    objects = [uuid.uuid4() for _ in range(10)]

    def produce() -> None:
        for obj in objects:
            producer.send('default', obj)

        producer.close()

    def consume() -> None:
        received = []

        for _, obj in zip(objects, consumer):
            assert isinstance(obj, Proxy)
            received.append(obj)

        assert received == objects

        consumer.close()

    pthread = threading.Thread(target=produce)
    cthread = threading.Thread(target=consume)

    cthread.start()
    pthread.start()

    pthread.join(timeout=5)
    cthread.join(timeout=5)


def test_context_manager(store: Store[LocalConnector]) -> None:
    publisher, subscriber = create_pubsub_pair()

    with StreamProducer(store, publisher) as producer:
        with StreamConsumer(store, subscriber) as consumer:
            producer.send('default', 'value')
            assert next(consumer) == 'value'


def test_close_without_closing_connectors(
    store: Store[LocalConnector],
) -> None:
    publisher, subscriber = create_pubsub_pair()

    producer = StreamProducer(store, publisher)
    consumer = StreamConsumer(store, subscriber)

    producer.close(store=False, publisher=False)
    consumer.close(store=False, subscriber=False)

    # Reuse store, publisher, subscriber
    with StreamProducer(store, publisher) as producer:
        with StreamConsumer(store, subscriber) as consumer:
            producer.send('default', 'value')
            assert next(consumer) == 'value'

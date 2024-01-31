from __future__ import annotations

import pathlib
import queue
import threading
import uuid
from typing import Generator

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store import store_registration
from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer
from proxystore.stream.shims.queue import QueuePublisher
from proxystore.stream.shims.queue import QueueSubscriber


def create_pubsub_pair(
    topic: str | None = None,
) -> tuple[QueuePublisher, QueueSubscriber]:
    topic = 'default' if topic is None else topic
    queue_: queue.Queue[bytes] = queue.Queue()

    publisher = QueuePublisher({topic: queue_})
    subscriber = QueueSubscriber(queue_)

    return publisher, subscriber


@pytest.fixture()
def store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    with Store('stream-test-fixture', FileConnector(str(tmp_path))) as store:
        with store_registration(store):
            yield store


def test_basic_interface(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    producer = StreamProducer[uuid.UUID](publisher, {topic: store})
    consumer = StreamConsumer[uuid.UUID](subscriber)

    objects = [uuid.uuid4() for _ in range(10)]

    def produce() -> None:
        for obj in objects:
            producer.send('default', obj)

        # Let the consumer handle closing the store
        producer.close(stores=False)

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


def test_context_manager(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    with StreamProducer[str](publisher, {topic: store}) as producer:
        with StreamConsumer[str](subscriber) as consumer:
            producer.send('default', 'value')
            assert next(consumer) == 'value'


def test_close_without_closing_connectors(
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    producer = StreamProducer[str](publisher, {topic: store})
    consumer = StreamConsumer[str](subscriber)

    producer.close(stores=False, publisher=False)
    consumer.close(stores=False, subscriber=False)

    # Reuse store, publisher, subscriber
    with StreamProducer[str](publisher, {topic: store}) as producer:
        with StreamConsumer[str](subscriber) as consumer:
            producer.send('default', 'value')
            assert next(consumer) == 'value'


def test_producer_close_topic(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    with StreamProducer[str](publisher, {topic: store}) as producer:
        with StreamConsumer[str](subscriber) as consumer:
            producer.close_topics('default')

            with pytest.raises(StopIteration):
                consumer.next()


def test_use_and_register_default_store(tmp_path: pathlib.Path) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    store = Store(
        'test-use-and-register-default-store',
        FileConnector(str(tmp_path)),
    )

    with StreamProducer[str](publisher, {None: store}) as producer:
        with StreamConsumer[str](subscriber) as consumer:
            producer.send(topic, 'value')

            assert get_store(store.name) is None
            consumer.next()
            assert get_store(store.name) is not None

    # Should get unregistered when closed
    assert get_store(store.name) is None


def test_missing_store_mapping_error(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    with StreamProducer[str](publisher, {'default': store}) as producer:
        with pytest.raises(ValueError, match='other'):
            producer.send('other', 'value')

    subscriber.close()

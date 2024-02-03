from __future__ import annotations

import math
import pathlib
import queue
import threading
import uuid
from typing import Any
from typing import Generator

import pytest

from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import Store
from proxystore.store import store_registration
from proxystore.stream.events import bytes_to_event
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectEvent
from proxystore.stream.exceptions import TopicClosedError
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


@pytest.mark.parametrize('batch_size', (1, 2, 3))
def test_simple_stream(batch_size: int, store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    producer = StreamProducer[uuid.UUID](
        publisher,
        {topic: store},
        batch_size=batch_size,
    )
    consumer = StreamConsumer[uuid.UUID](subscriber)

    objects = [uuid.uuid4() for _ in range(10)]

    def produce() -> None:
        for obj in objects:
            producer.send('default', obj)

        # Let the consumer handle closing the store
        producer.flush()
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
            producer.close_topics(topic)

            with pytest.raises(StopIteration):
                consumer.next()


def test_error_sending_to_closed_topic(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    with StreamProducer[str](publisher, {topic: store}) as producer:
        producer.close_topics(topic)
        with pytest.raises(TopicClosedError):
            producer.send(topic, 'value')

    subscriber.close()


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


@pytest.mark.parametrize('toggle_side', (True, False))
def test_filtering_stream(
    toggle_side: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    def filter_(metadata: dict[str, Any] | None) -> bool:
        assert metadata is not None
        return metadata['index'] % 2 != 0

    producer = StreamProducer[int](
        publisher,
        {topic: store},
        filter_=filter_ if toggle_side else None,
    )
    consumer = StreamConsumer[int](
        subscriber,
        filter_=filter_ if not toggle_side else None,
    )

    for i in range(0, 10):
        producer.send(topic, i, metadata={'index': i}, evict=True)

    events = list(publisher._queues[topic].queue)  # type: ignore[union-attr]

    producer.close_topics(topic)

    indices = list(consumer.iter_objects())
    assert len(indices) == 5
    assert indices == list(range(0, 10, 2))

    # Check all events were evicted regardless of if they were filtered
    for event_bytes in events:
        batch = bytes_to_event(event_bytes)
        assert isinstance(batch, EventBatch)
        (event,) = batch.events
        assert isinstance(event, NewObjectEvent)
        assert not store.exists(event.get_key())

    producer.close()
    consumer.close()


@pytest.mark.parametrize('batch_size', (1, 2, 3))
def test_aggregator(batch_size: int, store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    producer = StreamProducer[int](
        publisher,
        {topic: store},
        aggregator=sum,
        batch_size=batch_size,
    )
    consumer = StreamConsumer[int](subscriber)

    count = 10
    for _ in range(count):
        producer.send(topic, 1, evict=True)
    producer.close_topics(topic)

    values = list(consumer.iter_objects())
    assert len(values) == math.ceil(count / batch_size)
    assert sum(values) == count

    producer.close()
    consumer.close()


@pytest.mark.parametrize('evict', (True, False))
def test_consumer_next_object_evicts(
    evict: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    producer = StreamProducer[str](publisher, {topic: store})
    consumer = StreamConsumer[str](subscriber)

    producer.send(topic, 'value', evict=evict)

    # This assumes knowledge of the internal details of the publisher
    # but we need to get the key associated with the event we just published.
    events = list(publisher._queues[topic].queue)  # type: ignore[union-attr]
    batch = bytes_to_event(events[0])
    assert isinstance(batch, EventBatch)
    (event,) = batch.events
    assert isinstance(event, NewObjectEvent)
    key = event.get_key()

    assert store.exists(key)
    assert consumer.next_object() == 'value'
    assert store.exists(key) != evict

    producer.close()
    consumer.close()


def test_consumer_next_object_missing(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_pubsub_pair(topic)

    producer = StreamProducer[str](publisher, {topic: store})
    consumer = StreamConsumer[str](subscriber)

    producer.send(topic, 'value')

    # This assumes knowledge of the internal details of the publisher
    # but we need to get the key associated with the event we just published.
    events = list(publisher._queues[topic].queue)  # type: ignore[union-attr]
    batch = bytes_to_event(events[0])
    assert isinstance(batch, EventBatch)
    (event,) = batch.events
    assert isinstance(event, NewObjectEvent)
    key = event.get_key()

    store.evict(key)
    with pytest.raises(ValueError, match='returned None'):
        consumer.next_object()

    producer.close()
    consumer.close()

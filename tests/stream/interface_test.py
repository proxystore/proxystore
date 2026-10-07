from __future__ import annotations

import math
import pathlib
import threading
import uuid
from collections.abc import Generator
from typing import Any

import pytest

import proxystore.store
from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store import Store
from proxystore.stream import StreamConsumer
from proxystore.stream import StreamProducer
from proxystore.stream.events import bytes_to_event
from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectKeyEvent
from proxystore.stream.exceptions import TopicClosedError
from proxystore.stream.protocols import Publisher
from proxystore.stream.protocols import Subscriber
from testing.stream import create_event_pubsub_pair
from testing.stream import create_message_pubsub_pair


@pytest.fixture
def store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    with Store(FileConnector(tmp_path)) as store:
        yield store


@pytest.mark.parametrize('batch_size', (1, 2, 3))
@pytest.mark.parametrize('use_event_queue', (True, False))
@pytest.mark.parametrize('use_store', (True, False))
def test_stream_basics(
    batch_size: int,
    use_event_queue: bool,
    use_store: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher: Publisher
    subscriber: Subscriber

    if use_event_queue:
        publisher, subscriber = create_event_pubsub_pair(topic)
    else:
        publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[uuid.UUID](
        publisher,
        batch_size=batch_size,
        stores={topic: store if use_store else None},
    )
    consumer = StreamConsumer[uuid.UUID](subscriber)

    objects = [uuid.uuid4() for _ in range(10)]

    def produce() -> None:
        for obj in objects:
            producer.send('default', obj)

        producer.flush()

    def consume() -> None:
        received = []

        for _, obj in zip(objects, consumer, strict=False):
            if use_store:
                assert isinstance(obj, Proxy)
            received.append(obj)

        assert received == objects

    pthread = threading.Thread(target=produce)
    cthread = threading.Thread(target=consume)

    cthread.start()
    pthread.start()

    pthread.join(timeout=5)
    cthread.join(timeout=5)

    producer.close(stores=True)
    consumer.close(stores=True)


def test_context_manager(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    with (
        StreamProducer[str](publisher, stores={topic: store}) as producer,
        StreamConsumer[str](subscriber) as consumer,
    ):
        producer.send('default', 'value')
        assert next(consumer) == 'value'


def test_close_without_closing_connectors(
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[str](publisher, stores={topic: store})
    consumer = StreamConsumer[str](subscriber)

    producer.close(stores=False, publisher=False)
    consumer.close(stores=False, subscriber=False)

    # Reuse store, publisher, subscriber
    with (
        StreamProducer[str](publisher, stores={topic: store}) as producer,
        StreamConsumer[str](subscriber) as consumer,
    ):
        producer.send('default', 'value')
        assert next(consumer) == 'value'


@pytest.mark.parametrize('use_event_queue', (True, False))
def test_producer_close_topic(
    use_event_queue: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher: Publisher
    subscriber: Subscriber

    if use_event_queue:
        publisher, subscriber = create_event_pubsub_pair(topic)
    else:
        publisher, subscriber = create_message_pubsub_pair(topic)

    with (
        StreamProducer[str](publisher, stores={topic: store}) as producer,
        StreamConsumer[str](subscriber) as consumer,
    ):
        producer.close_topics(topic)

        with pytest.raises(StopIteration):  # pragma: <3.14 cover
            consumer.next()


def test_error_sending_to_closed_topic(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    with StreamProducer[str](publisher, stores={topic: store}) as producer:
        producer.close_topics(topic)
        with pytest.raises(TopicClosedError):
            producer.send(topic, 'value')

    subscriber.close()


def test_use_and_register_default_store(tmp_path: pathlib.Path) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    store = Store(FileConnector(tmp_path))

    producer = StreamProducer[str](publisher, default_store=store)
    consumer = StreamConsumer[str](subscriber)

    producer.send(topic, 'value')

    # Unregister the store of the producer so the consumer must create
    # a new store, as would happen in a different process.
    proxystore.store._registry.unregister(store)
    assert store.id not in proxystore.store._registry._stores
    consumer.next()
    consumer_store = proxystore.store._registry._stores[store.id]
    assert consumer_store is not store

    producer.close(stores=True)
    consumer.close(stores=True)

    # Should get unregistered when closed
    assert store.id not in proxystore.store._registry._stores


def test_topic_with_multiple_stores(tmp_path: pathlib.Path) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    with (
        Store(FileConnector(tmp_path / 'store1')) as store1,
        Store(FileConnector(tmp_path / 'store2')) as store2,
    ):
        producer1 = StreamProducer[str](publisher, default_store=store1)
        producer2 = StreamProducer[str](publisher, default_store=store2)
        consumer = StreamConsumer[str](subscriber)

        producer1.send(topic, 'value1')
        producer2.send(topic, 'value2')
        producer1.close_topics(topic)

        assert list(consumer.iter_objects()) == ['value1', 'value2']

        consumer.close()
        producer1.close()


def test_consumer_caches_store_without_id(
    store: Store[FileConnector],
) -> None:
    _, subscriber = create_message_pubsub_pair('default')
    consumer = StreamConsumer[str](subscriber)

    key = store.put('value')
    config = store.config().model_copy(update={'id': None})
    event = NewObjectKeyEvent.from_key(
        key,
        evict=False,
        metadata={},
        store_config=config,
        topic='default',
    )
    new_store = consumer._get_store(event)
    assert new_store is not store
    assert consumer._get_store(event) is new_store

    new_store.close()
    consumer.close()


@pytest.mark.parametrize('toggle_side', (True, False))
def test_filtering_stream(
    toggle_side: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    def keep(metadata: dict[str, Any] | None) -> bool:
        assert metadata is not None
        return metadata['index'] % 2 == 0

    producer = StreamProducer[int](
        publisher,
        stores={topic: store},
        keep=keep if toggle_side else None,
    )
    consumer = StreamConsumer[int](
        subscriber,
        keep=keep if not toggle_side else None,
    )

    for i in range(10):
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
        assert isinstance(event, NewObjectKeyEvent)
        assert not store.exists(event.get_key())

    producer.close()
    consumer.close()


@pytest.mark.parametrize('batch_size', (1, 2, 3))
def test_aggregator(batch_size: int, store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[int](
        publisher,
        aggregator=sum,
        batch_size=batch_size,
        stores={topic: store},
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


@pytest.mark.parametrize('use_store', (True, False))
def test_iter_with_metadata(
    use_store: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[int](
        publisher,
        stores={topic: store if use_store else None},
    )
    consumer = StreamConsumer[int](subscriber)

    count = 10
    for i in range(count):
        producer.send(topic, i, metadata={'value': i})
    producer.close_topics(topic)

    for metadata, proxy_or_item in consumer.iter_with_metadata():
        assert metadata['value'] == proxy_or_item

    producer.close()
    consumer.close()


@pytest.mark.parametrize('use_store', (True, False))
def test_iter_objects_with_metadata(
    use_store: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[int](
        publisher,
        stores={topic: store if use_store else None},
    )
    consumer = StreamConsumer[int](subscriber)

    count = 10
    for i in range(count):
        producer.send(topic, i, metadata={'value': i})
    producer.close_topics(topic)

    for metadata, obj in consumer.iter_objects_with_metadata():
        assert metadata['value'] == obj

    producer.close()
    consumer.close()


@pytest.mark.parametrize('evict', (True, False))
def test_consumer_next_object_evicts(
    evict: bool,
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[str](publisher, stores={topic: store})
    consumer = StreamConsumer[str](subscriber)

    producer.send(topic, 'value', evict=evict)

    # This assumes knowledge of the internal details of the publisher
    # but we need to get the key associated with the event we just published.
    events = list(publisher._queues[topic].queue)  # type: ignore[union-attr]
    batch = bytes_to_event(events[0])
    assert isinstance(batch, EventBatch)
    (event,) = batch.events
    assert isinstance(event, NewObjectKeyEvent)
    key = event.get_key()

    assert store.exists(key)
    assert consumer.next_object() == 'value'
    assert store.exists(key) != evict

    producer.close()
    consumer.close()


def test_consumer_next_object_missing(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    producer = StreamProducer[str](publisher, stores={topic: store})
    consumer = StreamConsumer[str](subscriber)

    producer.send(topic, 'value')

    # This assumes knowledge of the internal details of the publisher
    # but we need to get the key associated with the event we just published.
    events = list(publisher._queues[topic].queue)  # type: ignore[union-attr]
    batch = bytes_to_event(events[0])
    assert isinstance(batch, EventBatch)
    (event,) = batch.events
    assert isinstance(event, NewObjectKeyEvent)
    key = event.get_key()

    store.evict(key)
    with pytest.raises(ValueError, match='does not contain an object'):
        consumer.next_object()

    producer.close()
    consumer.close()


def test_consumer_next_object_none(store: Store[FileConnector]) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    with (
        StreamProducer[None](publisher, stores={topic: store}) as producer,
        StreamConsumer[None](subscriber) as consumer,
    ):
        producer.send(topic, None)
        assert consumer.next_object() is None


def test_producer_sends_end_of_stream_once(
    store: Store[FileConnector],
) -> None:
    topic = 'default'
    publisher, subscriber = create_message_pubsub_pair(topic)

    with StreamProducer[str](publisher, stores={topic: store}) as producer:
        producer.send(topic, 'value')
        producer.close_topics(topic)
        producer.flush()
        producer.flush_topic(topic)
        producer.close_topics(topic)

        # This assumes knowledge of the internal details of the publisher.
        messages = list(publisher._queues[topic].queue)  # type: ignore[union-attr]
        events = []
        for message in messages:
            batch = bytes_to_event(message)
            assert isinstance(batch, EventBatch)
            events.extend(batch.events)
        assert sum(isinstance(e, EndOfStreamEvent) for e in events) == 1

    subscriber.close()

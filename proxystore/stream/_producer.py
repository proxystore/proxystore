from __future__ import annotations

import dataclasses
import logging
import sys
from collections import defaultdict
from collections.abc import Iterable
from collections.abc import Mapping
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.store import Store
from proxystore.store import unregister_store
from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import Event
from proxystore.stream.events import event_to_bytes
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectEvent
from proxystore.stream.events import NewObjectKeyEvent
from proxystore.stream.exceptions import TopicClosedError
from proxystore.stream.filters import NullFilter
from proxystore.stream.protocols import EventPublisher
from proxystore.stream.protocols import Filter
from proxystore.stream.protocols import MessagePublisher
from proxystore.stream.protocols import Publisher

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclasses.dataclass(frozen=True)
class _BufferedObject(Generic[T]):
    obj: T
    evict: bool
    metadata: dict[str, Any]


@dataclasses.dataclass
class _TopicBuffer(Generic[T]):
    objects: list[_BufferedObject[T]]
    closed: bool


class StreamProducer(Generic[T]):
    """Stream producer interface.

    This interface enables streaming objects in a manner which decouples
    bulk object transfer from event notifications. Topics can be associated
    with a [`Store`][proxystore.store.Store] which will be used for bulk
    object storage and communication. Event metadata, including the key to
    the object in the store, is communicated through a message broker using
    the [`Publisher`][proxystore.stream.protocols.Publisher] and
    [`Subscriber`][proxystore.stream.protocols.Subscriber] interfaces.
    The associated [`StreamConsumer`][proxystore.stream.StreamConsumer] can
    be used to iterate on proxies of objects from the stream.

    This interface can also be used without a
    [`Store`][proxystore.store.Store] in which case the object is
    included in the event metadata and communicated directly through the
    message broker.

    Note:
        The [`StreamProducer`][proxystore.stream.StreamProducer] can
        be used as a context manager.

        ```python
        with StreamProducer(...) as stream:
            for item in ...:
                stream.send(item)
        ```

    Tip:
        This class is generic, so it is recommended that the type of objects
        in the stream be annotated appropriately. This is useful for enabling
        a static type checker to validate that the correct object types are
        published to the stream.
        ```python
        producer = StreamProducer[str](...)
        # mypy will raise an error that StreamProducer.send() expects a str
        # but got a list[int].
        producer.send('default', [1, 2, 3])
        ```

    Warning:
        The producer is not thread-safe.

    Attributes:
        publisher: Publisher interface.

    Args:
        publisher: Object which implements one of the
            [`Publisher`][proxystore.stream.protocols.Publisher] protocols.
            Used to publish event messages when new objects are added to
            the stream.
        aggregator: Optional aggregator which takes as input the batch of
            objects and returns a single object of the same type when invoked.
            The size of the batch passed to the aggregator is controlled by
            the `batch_size` parameter. When aggregation is used, the metadata
            associated with the aggregated object will be the union of each
            metadata dict from each object in the batch.
        batch_size: Batch size used for batching and aggregation.
        default_store: Specify the default
            [`Store`][proxystore.store.Store] to be used with topics
            not explicitly set in `stores`. If no default is provided, objects
            are included directly in the event.
        filter_: Optional filter to apply prior to sending objects to the
            stream. If the filter returns `True` for a given object's
            metadata, the object will *not* be sent to the stream. The filter
            is applied before aggregation or batching.
        stores: Mapping from topic names to an optional
            [`Store`][proxystore.store.Store] instance used to store
            and communicate serialized objects streamed to that topic.
            If the value associated with a topic is `None`, the object is
            included directly in the event.
    """

    def __init__(
        self,
        publisher: Publisher,
        *,
        aggregator: Callable[[list[T]], T] | None = None,
        batch_size: int = 1,
        default_store: Store[Any] | None = None,
        filter_: Filter | None = None,
        stores: Mapping[str, Store[Any] | None] | None = None,
    ) -> None:
        self.publisher = publisher
        self._default_store = default_store
        self._aggregator = aggregator
        self._batch_size = batch_size
        self._filter: Filter = filter_ if filter_ is not None else NullFilter()
        self._stores = stores

        # Mapping between topic and buffers
        self._buffer: dict[str, _TopicBuffer[T]] = defaultdict(
            lambda: _TopicBuffer([], False),
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _get_store(self, topic: str) -> Store[Any] | None:
        if self._stores is not None and topic in self._stores:
            return self._stores[topic]
        return self._default_store

    def _send_event(self, batch: EventBatch) -> None:
        if isinstance(self.publisher, EventPublisher):
            self.publisher.send_events(batch)
        elif isinstance(self.publisher, MessagePublisher):
            message = event_to_bytes(batch)
            self.publisher.send_message(batch.topic, message)
        else:
            raise AssertionError('Unreachable.')

    def close(
        self,
        *,
        topics: Iterable[str] = (),
        publisher: bool = True,
        stores: bool = False,
    ) -> None:
        """Close the producer.

        Warning:
            Objects buffered in an incomplete batch will be lost. Call
            [`flush()`][proxystore.stream.StreamProducer] to ensure
            that all objects are sent before closing, or pass a list of
            topics to flush and close.

        Warning:
            By default, this will close the
            [`Publisher`][proxystore.stream.protocols.Publisher] interface,
            but will **not** close the [`Store`][proxystore.store.Store]
            interfaces.

        Args:
            topics: Topics to send end of stream events to. Equivalent to
                calling [`close_topics()`][proxystore.stream.StreamProducer.close_topics]
                first.
            publisher: Close the
                [`Publisher`][proxystore.stream.protocols.Publisher] interface.
            stores: Close and [unregister][proxystore.store.unregister_store]
                the [`Store`][proxystore.store.Store] instances.
        """  # noqa: E501
        self.close_topics(*topics)
        if stores:
            known_stores = {self._default_store}
            if self._stores is not None:
                known_stores.update(self._stores.values())
            for store in known_stores:
                if store is not None:
                    store.close()
                    unregister_store(store)
        if publisher:
            self.publisher.close()

    def close_topics(self, *topics: str) -> None:
        """Send an end of stream event to each topic.

        A [`StreamConsumer`][proxystore.stream.StreamConsumer]
        will raise a [`StopIteration`][StopIteration] exception when an
        end of stream event is received. The end of stream event is still
        ordered, however, so all prior sent events will be consumed first
        before the end of stream event is propagated.

        Note:
            This will flush the topic buffer.

        Args:
            topics: Topics to send end of stream events to.
        """
        for topic in topics:
            self._buffer[topic].closed = True
            self.flush_topic(topic)

    def flush(self) -> None:
        """Flush batch buffers for all topics."""
        for topic in self._buffer:
            self.flush_topic(topic)

    def flush_topic(self, topic: str) -> None:
        """Flush the batch buffer for a topic.

        This method:

        1. Pops the current batch of objects off the topic buffer.
        2. Applies the aggregator to the batch if one was provided.
        3. Puts the batch of objects in the
           [`Store`][proxystore.store.Store].
        4. Creates a new batch event using the keys returned by the store and
           additional metadata.
        5. Publishes the event to the stream via the
           [`Publisher`][proxystore.stream.protocols.Publisher].

        Args:
            topic: Topic to flush.
        """
        objects = self._buffer[topic].objects
        closed = self._buffer[topic].closed

        if len(objects) == 0 and not closed:
            # No events to send so quick return
            return

        # Reset buffer
        self._buffer[topic].objects = []

        if self._aggregator is not None and len(objects) > 0:
            obj = self._aggregator([item.obj for item in objects])
            evict = any([item.evict for item in objects])
            metadata: dict[str, Any] = {}
            for item in objects:
                metadata.update(item.metadata)
            objects = [_BufferedObject(obj, evict, metadata)]

        events: list[Event] = []
        store = self._get_store(topic)

        if len(objects) > 0 and store is not None:
            keys = store.put_batch([item.obj for item in objects])
            config = store.config()

            for key, item in zip(keys, objects):
                events.append(
                    NewObjectKeyEvent.from_key(
                        key,
                        evict=item.evict,
                        metadata=item.metadata,
                        store_config=config,
                        topic=topic,
                    ),
                )
        elif len(objects) > 0 and store is None:
            for item in objects:
                events.append(
                    NewObjectEvent(
                        topic=topic,
                        obj=item.obj,
                        metadata=item.metadata,
                    ),
                )

        if closed:
            events.append(EndOfStreamEvent(topic))

        # If there are no new events and the stream wasn't closed we should
        # have early exited
        assert len(events) > 0
        batch_event = EventBatch(topic, events)
        self._send_event(batch_event)

    def send(
        self,
        topic: str,
        obj: T,
        *,
        evict: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Send an item to the stream.

        This method:

        1. Applies the filter to the metadata associated with this event,
           skipping streaming this object if the filter returns `True`.
        2. Adds the object to the internal event buffer for this topic.
        3. Flushes the event buffer once the batch size is reached.

        Warning:
            Careful consideration should be given to the setting of the
            `evict` flag. When set to `True`, the corresponding proxy
            yielded by the consumer of the stream will only be resolvable
            once. If you encounter unexpected
            [`ProxyResolveMissingKeyError`][proxystore.store.exceptions.ProxyResolveMissingKeyError]
            errors, it may be due to proxies from the stream being resolved
            multiple times but the first resolve triggered an eviction
            of the underlying data.

        Args:
            topic: Stream topic to send the object to.
            obj: Object to send via the stream.
            evict: Evict the object from the
                [`Store`][proxystore.store.Store] once the object is
                consumed by a
                [`StreamConsumer`][proxystore.stream.StreamConsumer].
                Set to `False` if a single object in the stream will be
                consumed by multiple consumers. Note that when set to `False`,
                data eviction must be handled manually. This parameter is
                ignored if a [`Store`][proxystore.store.base.Store] is not
                mapped to this topic.
            metadata: Dictionary containing metadata about the object. This
                can be used by the producer or consumer to filter new
                object events. The default value `None` is replaced with an
                empty [`dict`][dict].

        Raises:
            TopicClosedError: If the `topic` has already been closed via
                [`close_topics()`][proxystore.stream.StreamProducer.close_topics].
            ValueError: If a store associated with `topic` is not found
                in the mapping of topics to stores nor a default store is
                provided.
        """
        if self._buffer[topic].closed:
            raise TopicClosedError(f'Topic "{topic}" has been closed.')

        metadata = metadata if metadata is not None else {}
        if self._filter(metadata):
            return

        item = _BufferedObject(obj, evict, metadata)
        self._buffer[topic].objects.append(item)

        if len(self._buffer[topic].objects) >= self._batch_size:
            self.flush_topic(topic)

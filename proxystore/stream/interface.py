"""Stream producer and consumer interfaces.

Note:
    The [StreamProducer][proxystore.stream.interface.StreamProducer]
    and [StreamConsumer][proxystore.stream.interface.StreamConsumer]
    are re-exported in [`proxystore.stream`][proxystore.stream] for
    convenience.
"""
from __future__ import annotations

import dataclasses
import logging
import sys
import threading
from collections import defaultdict
from types import TracebackType
from typing import Any
from typing import Callable
from typing import cast
from typing import Generator
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import NamedTuple
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.stream.events import bytes_to_event
from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import Event
from proxystore.stream.events import event_to_bytes
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectEvent
from proxystore.stream.exceptions import TopicClosedError
from proxystore.stream.filters import NullFilter
from proxystore.stream.protocols import Filter
from proxystore.stream.protocols import Publisher
from proxystore.stream.protocols import Subscriber

logger = logging.getLogger(__name__)

_consumer_get_store_lock = threading.Lock()

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
    """Proxy stream producer interface.

    Note:
        The [`StreamProducer`][proxystore.stream.interface.StreamProducer] can
        be used as a context manager.

        ```python
        with StreamProducer(...) as stream:
            for item in ...:
                stream.send(item)
        ```

    Warning:
        The producer is not thread-safe.

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

    Args:
        publisher: Object which implements the
            [`Publisher`][proxystore.stream.protocols.Publisher] protocol.
            Used to publish event messages when new objects are added to
            the stream.
        stores: Mapping from topic names to the
            [`Store`][proxystore.store.base.Store] instance used to store
            and communicate serialized objects streamed to that topic.
            The `None` topic can be used to specify a default
            [`Store`][proxystore.store.base.Store] used for topics
            not present in this mapping.
        aggregator: Optional aggregator which takes as input the batch of
            objects and returns a single object of the same type when invoked.
            The size of the batch passed to the aggregator is controlled by
            the `batch_size` parameter. When aggregation is used, the metadata
            associated with the aggregated object will be the union of each
            metadata dict from each object in the batch.
        batch_size: Batch size used for batching and aggregation.
        filter_: Optional filter to apply prior to sending objects to the
            stream. If the filter returns `True` for a given object's
            metadata, the object will *not* be sent to the stream. The filter
            is applied before aggregation or batching.
    """

    def __init__(
        self,
        publisher: Publisher,
        stores: Mapping[str | None, Store[Any]],
        *,
        aggregator: Callable[[list[T]], T] | None = None,
        batch_size: int = 1,
        filter_: Filter | None = None,
    ) -> None:
        self._publisher = publisher
        self._stores = stores
        self._aggregator = aggregator
        self._batch_size = batch_size
        self._filter: Filter = filter_ if filter_ is not None else NullFilter()

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

    def close(
        self,
        *,
        topics: Iterable[str] = (),
        publisher: bool = True,
        stores: bool = True,
    ) -> None:
        """Close the producer.

        Warning:
            Objects buffered in an incomplete batch will be lost. Call
            [`flush()`][proxystore.stream.interface.StreamProducer] to ensure
            that all objects are sent before closing.

        Warning:
            By default, this will also call `close()` on the
            [`Store`][proxystore.store.base.Store] and
            [`Publisher`][proxystore.stream.protocols.Publisher] interfaces.

        Args:
            topics: Topics to send end of stream events to. Equivalent to
                calling [`close_topics()`][proxystore.stream.interface.StreamProducer.close_topics]
                first.
            publisher: Close the
                [`Publisher`][proxystore.stream.protocols.Publisher] interface.
            stores: Close and [unregister][proxystore.store.unregister_store]
                the [`Store`][proxystore.store.base.Store] instances.
        """  # noqa: E501
        self.close_topics(*topics)
        if stores:
            for store in self._stores.values():
                store.close()
                unregister_store(store)
        if publisher:
            self._publisher.close()

    def close_topics(self, *topics: str) -> None:
        """Send publish an end of stream event to each topic.

        A [`StreamConsumer`][proxystore.stream.interface.StreamConsumer]
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
           [`Store`][proxystore.store.base.Store].
        4. Creates a new batch event using the keys returned by the store and
           additional metadata.
        5. Publishes the event to the stream via the
           [`Publisher`][proxystore.stream.protocols.Publisher].

        Args:
            topic: Topic to flush.

        ValueError: if a store associated with `topic` is not found
            in the mapping of topics to stores nor a default store is
            provided.
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

        if topic in self._stores:
            store = self._stores[topic]
        elif None in self._stores:
            store = self._stores[None]
        else:
            raise ValueError(
                f'No store associated with topic "{topic}" found or '
                'default store.',
            )

        keys = store.put_batch([item.obj for item in objects])

        events: list[Event] = [
            NewObjectEvent.from_key(
                key,
                evict=item.evict,
                metadata=item.metadata,
            )
            for key, item in zip(keys, objects)
        ]

        if closed:
            events.append(EndOfStreamEvent())

        batch_event = EventBatch(events, topic, store.config())
        message = event_to_bytes(batch_event)
        self._publisher.send(topic, message)

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
                [`Store`][proxystore.store.base.Store] once the object is
                consumed by a
                [`StreamConsumer`][proxystore.stream.interface.StreamConsumer].
                Set to `False` if a single object in the stream will be
                consumed by multiple consumers. Note that when set to `False`,
                data eviction must be handled manually.
            metadata: Dictionary containing metadata about the object. This
                can be used by the producer or consumer to filter new
                object events. The default value `None` is replaced with an
                empty [`dict`][dict].

        Raises:
            TopicClosedError: if the `topic` has already been closed via
                [`close_topics()`][proxystore.stream.interface.StreamProducer.close_topics].
            ValueError: if a store associated with `topic` is not found
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


class _EventInfo(NamedTuple):
    event: NewObjectEvent
    topic: str
    store_config: dict[str, Any]


class StreamConsumer(Generic[T]):
    """Proxy stream consumer interface.

    This interface acts as an iterator that will yield items from the stream
    until the stream is closed.

    Note:
        The [`StreamConsumer`][proxystore.stream.interface.StreamConsumer] can
        be used as a context manager.

        ```python
        with StreamConsumer(...) as stream:
            for item in stream:
                ...
        ```

    Tip:
        This class is generic, so it is recommended that the type of objects
        in the stream be annotated appropriately.
        ```python
        consumer = StreamConsumer[str](...)
        reveal_type(consumer.next())
        # Proxy[str]
        ```
        If the stream is heterogeneous or objects types are not known ahead
        of time, it may be appropriate to annotate the stream with
        [`Any`][typing.Any].
        ```python
        consumer = StreamConsumer[Any](...)
        reveal_type(consumer.next())
        # Proxy[Any]
        ```

    Warning:
        If you encounter unexpected
        [`ProxyResolveMissingKeyError`][proxystore.store.exceptions.ProxyResolveMissingKeyError]
        errors, it may be due to proxies from the stream being resolved
        multiple times but the first resolve triggered an eviction
        of the underlying data. If this is the case, confirm that the
        setting of the `evict` flag on
        [`StreamProducer.send()`][proxystore.stream.interface.StreamProducer.send]
        is set correctly and the there is not code incidentally resolving
        proxies before you expect.

    Note:
        The consumer is not thread-safe.

    Args:
        subscriber: Object which implements the
            [`Subscriber`][proxystore.stream.protocols.Subscriber] protocol.
            Used to listen for new event messages indicating new objects
            in the stream.
        filter_: Optional filter to apply to event metadata received from the
            stream. If the filter returns `True`, the event will be
            dropped (i.e., not yielded back to the user), and the object
            associated with that event will be deleted if the `evict` flag
            was set on the producer side.
    """

    def __init__(
        self,
        subscriber: Subscriber,
        *,
        filter_: Filter | None = None,
    ) -> None:
        self._subscriber = subscriber
        self._stores: dict[str, Store[Any]] = {}
        self._filter: Filter = filter_ if filter_ is not None else NullFilter()

        self._current_batch: EventBatch | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __iter__(self) -> Self:
        """Return an iterator that will yield proxies of stream objects."""
        return self

    def __next__(self) -> Proxy[T]:
        """Alias for [`next()`][proxystore.stream.interface.StreamConsumer.next]."""  # noqa: E501
        return self.next()

    def close(self, *, stores: bool = True, subscriber: bool = True) -> None:
        """Close the consumer.

        Warning:
            By default, this will also call `close()` on the
            [`Store`][proxystore.store.base.Store] and
            [`Publisher`][proxystore.stream.protocols.Publisher] interfaces.

        Args:
            stores: Close and [unregister][proxystore.store.unregister_store]
                the [`Store`][proxystore.store.base.Store] instances
                used to resolve objects consumed from the stream.
            subscriber: Close the
                [`Subscriber`][proxystore.stream.protocols.Subscriber]
                interface.
        """
        if stores:
            for store in self._stores.values():
                store.close()
                unregister_store(store)
        if subscriber:
            self._subscriber.close()

    def _get_store(self, event_info: _EventInfo) -> Store[Any]:
        if event_info.topic in self._stores:
            return self._stores[event_info.topic]

        with _consumer_get_store_lock:
            store = get_store(event_info.store_config['name'])
            if store is None:
                store = Store.from_config(event_info.store_config)
                register_store(store)
            self._stores[event_info.topic] = store
            return store

    def _next_batch(self) -> EventBatch:
        message = next(self._subscriber)
        event = bytes_to_event(message)
        if isinstance(event, EventBatch):
            return event
        else:
            raise AssertionError('Unreachable.')

    def _next_event(self) -> _EventInfo:
        if self._current_batch is None or len(self._current_batch.events) == 0:
            # Current batch does not exist or has been exhausted so block
            # on a new batch.
            self._current_batch = self._next_batch()
            # Reverse list of events so we can O(1) pop from end of list to
            # get the first event in time.
            self._current_batch.events = list(
                reversed(self._current_batch.events),
            )

        event = self._current_batch.events.pop()
        if isinstance(event, NewObjectEvent):
            return _EventInfo(
                event,
                self._current_batch.topic,
                self._current_batch.store_config,
            )
        elif isinstance(event, EndOfStreamEvent):
            raise StopIteration
        else:
            raise AssertionError('Unreachable.')

    def _next_event_with_filter(self) -> _EventInfo:
        while True:
            # _next_event() will propagate up a StopIteration exception
            # if the event was an end of stream event.
            event_info = self._next_event()
            event = event_info.event

            if self._filter(event.metadata):
                # Coverage in Python 3.8/3.9 does not mark the "else"
                # as covered but if you put else: assert False the some
                # tests fail there so it does get run
                if event.evict:  # pragma: no branch
                    # If an object gets filtered out by the consumer client,
                    # we still want to respect the evict flag to.
                    store = self._get_store(event_info)
                    key = event.get_key()
                    store.evict(key)
                continue

            return event_info

    def iter_objects(self) -> Generator[T, None, None]:
        """Return an iterator that will yield objects from the stream.

        Note:
            This is different from `iter(consumer)` which will yield
            proxies of objects in the stream.
        """
        while True:
            try:
                yield self.next_object()
            except StopIteration:
                return

    def next(self) -> Proxy[T]:
        """Return a proxy of the next object in the stream.

        Note:
            This method has the potential side effect of initializing and
            globally registering a new [`Store`][proxystore.store.base.Store]
            instance. This will happen at most once per topic because the
            producer can map topic names to
            [`Store`][proxystore.store.base.Store] instances. This class will
            keep track of the [`Store`][proxystore.store.base.Store] instances
            used by the stream and will close and unregister them when this
            class is closed.

        Raises:
            StopIteration: when an end of stream event is received from a
                producer. Note that this does not call
                [`close()`][proxystore.stream.interface.StreamConsumer.close].
        """
        event_info = self._next_event_with_filter()
        store = self._get_store(event_info)
        event = event_info.event
        key = event.get_key()

        proxy: Proxy[T] = store.proxy_from_key(key, evict=event.evict)
        return proxy

    def next_object(self) -> T:
        """Return the next object in the stream.

        Note:
            This method has the same potential side effects as
            [`next()`][proxystore.stream.interface.StreamConsumer.next].

        Raises:
            StopIteration: when an end of stream event is received from a
                producer. Note that this does not call
                [`close()`][proxystore.stream.interface.StreamConsumer.close].
            ValueError: if the store does not return an object using the key
                included in the object's event metadata.
        """
        event_info = self._next_event_with_filter()
        store = self._get_store(event_info)
        event = event_info.event
        key = event.get_key()

        obj = store.get(key)
        if obj is None:
            raise ValueError(
                f'Store(name="{store.name}") returned None for key={key}.',
            )

        if event.evict:
            store.evict(key)

        return cast(T, obj)

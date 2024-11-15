from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Generator
from types import TracebackType
from typing import Any
from typing import cast
from typing import Generic
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.proxy import ProxyOr
from proxystore.store import get_or_create_store
from proxystore.store import Store
from proxystore.store import unregister_store
from proxystore.stream.events import bytes_to_event
from proxystore.stream.events import EndOfStreamEvent
from proxystore.stream.events import EventBatch
from proxystore.stream.events import NewObjectEvent
from proxystore.stream.events import NewObjectKeyEvent
from proxystore.stream.filters import NullFilter
from proxystore.stream.protocols import EventSubscriber
from proxystore.stream.protocols import Filter
from proxystore.stream.protocols import MessageSubscriber
from proxystore.stream.protocols import Subscriber

logger = logging.getLogger(__name__)

_consumer_get_store_lock = threading.Lock()

T = TypeVar('T')


class StreamConsumer(Generic[T]):
    """Stream consumer interface.

    This interface acts as an iterator that will yield items from the stream
    until the stream is closed.

    Note:
        The [`StreamConsumer`][proxystore.stream.StreamConsumer] can
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
        # ProxyOr[str]
        reveal_type(consumer.next_object())
        # str
        ```
        If the stream is heterogeneous or objects types are not known ahead
        of time, it may be appropriate to annotate the stream with
        [`Any`][typing.Any].
        ```python
        consumer = StreamConsumer[Any](...)
        reveal_type(consumer.next())
        # ProxyOr[Any]
        ```

    Warning:
        If you encounter unexpected
        [`ProxyResolveMissingKeyError`][proxystore.store.exceptions.ProxyResolveMissingKeyError]
        errors, it may be due to proxies from the stream being resolved
        multiple times but the first resolve triggered an eviction
        of the underlying data. If this is the case, confirm that the
        setting of the `evict` flag on
        [`StreamProducer.send()`][proxystore.stream.StreamProducer.send]
        is set correctly and the there is not code incidentally resolving
        proxies before you expect.

    Warning:
        The consumer is not thread-safe.

    Attributes:
        subscriber: Subscriber interface.

    Args:
        subscriber: Object which implements one of the
            [`Subscriber`][proxystore.stream.protocols.Subscriber] protocols.
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
        self.subscriber = subscriber
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
        """Return an iterator that will yield stream items.

        The return type of items is based on that returned by
        [`next()`][proxystore.stream.StreamConsumer.next].
        """
        return self

    def __next__(self) -> ProxyOr[T]:
        """Alias for [`next()`][proxystore.stream.StreamConsumer.next]."""
        return self.next()

    def close(self, *, stores: bool = False, subscriber: bool = True) -> None:
        """Close the consumer.

        Warning:
            By default, this will close the
            [`Subscriber`][proxystore.stream.protocols.Subscriber] interface,
            but will **not** close the [`Store`][proxystore.store.Store]
            interfaces.

        Args:
            stores: Close and [unregister][proxystore.store.unregister_store]
                the [`Store`][proxystore.store.Store] instances
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
            self.subscriber.close()

    def _get_store(self, event: NewObjectKeyEvent) -> Store[Any]:
        with _consumer_get_store_lock:
            if event.topic in self._stores:
                return self._stores[event.topic]

            store = get_or_create_store(event.store_config, register=True)
            self._stores[event.topic] = store
            return store

    def _next_batch(self) -> EventBatch:
        if isinstance(self.subscriber, EventSubscriber):
            return next(self.subscriber)
        elif isinstance(self.subscriber, MessageSubscriber):
            message = next(self.subscriber)
            event = bytes_to_event(message)
            assert isinstance(event, EventBatch)
            return event
        else:
            raise AssertionError('Unreachable.')

    def _next_event(self) -> NewObjectEvent | NewObjectKeyEvent:
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
        if isinstance(event, (NewObjectEvent, NewObjectKeyEvent)):
            return event
        elif isinstance(event, EndOfStreamEvent):
            raise StopIteration
        else:
            raise AssertionError('Unreachable.')

    def _next_event_with_filter(self) -> NewObjectEvent | NewObjectKeyEvent:
        while True:
            # _next_event() will propagate up a StopIteration exception
            # if the event was an end of stream event.
            event = self._next_event()

            if self._filter(event.metadata):
                # Coverage in Python 3.8/3.9 does not mark the "else"
                # as covered but if you put else: assert False the some
                # tests fail there so it does get run
                if (  # pragma: no branch
                    isinstance(event, NewObjectKeyEvent) and event.evict
                ):
                    # If an object gets filtered out by the consumer client,
                    # we still want to respect the evict flag to.
                    store = self._get_store(event)
                    store.evict(event.get_key())

                continue

            return event

    def iter_with_metadata(
        self,
    ) -> Generator[tuple[dict[str, Any], ProxyOr[T]], None, None]:
        """Create an iterator that yields tuples of metadata and items.

        The return type of items is based on that returned by
        [`next()`][proxystore.stream.StreamConsumer.next].
        This is different from `iter(consumer)` which will yield *only* items,
        dropping the metadata.
        """
        while True:
            try:
                yield self.next_with_metadata()
            except StopIteration:
                return

    def iter_objects(self) -> Generator[T, None, None]:
        """Create an iterator that yields objects from the stream."""
        while True:
            try:
                yield self.next_object()
            except StopIteration:
                return

    def iter_objects_with_metadata(
        self,
    ) -> Generator[tuple[dict[str, Any], T], None, None]:
        """Create an iterator that yields tuples of metadata and objects."""
        while True:
            try:
                yield self.next_object_with_metadata()
            except StopIteration:
                return

    def next(self) -> ProxyOr[T]:
        """Get the next item in the stream.

        Note:
            This method has the potential side effect of initializing and
            globally registering a new [`Store`][proxystore.store.Store]
            instance. This will happen at most once per topic because the
            producer can map topic names to
            [`Store`][proxystore.store.Store] instances. This class will
            keep track of the [`Store`][proxystore.store.Store] instances
            used by the stream and will close and unregister them when this
            class is closed.

        Returns:
            [`Proxy[T]`][proxystore.proxy.Proxy] is returned if the topic \
            was associated with a [`Store`][proxystore.store.Store] \
            in the [`StreamProducer`][proxystore.stream.StreamProducer] \
            otherwise `T` is returned.

        Raises:
            StopIteration: When an end of stream event is received from a
                producer. Note that this does not call
                [`close()`][proxystore.stream.StreamConsumer.close].
        """
        _, proxy = self.next_with_metadata()
        return proxy

    def next_with_metadata(self) -> tuple[dict[str, Any], ProxyOr[T]]:
        """Get the next item with metadata in the stream.

        Note:
            This method has the same potential side effects as and return type
            as [`next()`][proxystore.stream.StreamConsumer.next].

        Returns:
            Dictionary of user-provided metadata associated with the object.
            Proxy of the next object in the stream.

        Raises:
            StopIteration: When an end of stream event is received from a
                producer. Note that this does not call
                [`close()`][proxystore.stream.StreamConsumer.close].
        """
        event = self._next_event_with_filter()

        if isinstance(event, NewObjectEvent):
            return event.metadata, cast(T, event.obj)
        elif isinstance(event, NewObjectKeyEvent):
            store = self._get_store(event)
            proxy: Proxy[T] = store.proxy_from_key(
                event.get_key(),
                evict=event.evict,
            )
            return event.metadata, proxy
        else:
            raise AssertionError('Unreachable.')

    def next_object(self) -> T:
        """Get the next object in the stream.

        Note:
            This method has the same potential side effects as
            [`next()`][proxystore.stream.StreamConsumer.next].

        Raises:
            StopIteration: When an end of stream event is received from a
                producer. Note that this does not call
                [`close()`][proxystore.stream.StreamConsumer.close].
            ValueError: If the store does not return an object using the key
                included in the object's event metadata.
        """
        _, obj = self.next_object_with_metadata()
        return obj

    def next_object_with_metadata(self) -> tuple[dict[str, Any], T]:
        """Get the next object with metadata in the stream.

        Note:
            This method has the same potential side effects as
            [`next()`][proxystore.stream.StreamConsumer.next].

        Returns:
            Dictionary of user-provided metadata associated with the object.
            Next object in the stream.

        Raises:
            StopIteration: When an end of stream event is received from a
                producer. Note that this does not call
                [`close()`][proxystore.stream.StreamConsumer.close].
        """
        event = self._next_event_with_filter()

        if isinstance(event, NewObjectEvent):
            return event.metadata, cast(T, event.obj)
        elif isinstance(event, NewObjectKeyEvent):
            store = self._get_store(event)
            key = event.get_key()
            obj = store.get(key)
            if obj is None:
                raise ValueError(
                    f'Store(name="{store.name}") returned None for key={key}.',
                )
            if event.evict:
                store.evict(key)
            return event.metadata, cast(T, obj)
        else:
            raise AssertionError('Unreachable.')

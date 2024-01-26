"""Proxy streaming interface.

Warning:
    The streaming interfaces are experimental and may change in future
    releases.

Tip:
    Checkout the [Streaming Guide](../guides/streaming.md) to learn more!
"""
from __future__ import annotations

import dataclasses
import json
import logging
import sys
import warnings
from types import TracebackType
from typing import Any
from typing import Generic
from typing import NamedTuple
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.pubsub.protocols import Publisher
from proxystore.pubsub.protocols import Subscriber
from proxystore.store.base import Store
from proxystore.utils.imports import get_class_path
from proxystore.utils.imports import import_class
from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'MultiConnector is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)

logger = logging.getLogger(__name__)


KeyT = TypeVar('KeyT', bound=NamedTuple)


@dataclasses.dataclass
class _Event(Generic[KeyT]):
    key_type: str
    raw_key: tuple[Any]
    evict: bool

    @classmethod
    def from_key(cls, key: KeyT, *, evict: bool = True) -> _Event[KeyT]:
        return _Event(
            key_type=get_class_path(type(key)),
            raw_key=tuple(key),
            evict=evict,
        )

    @classmethod
    def from_json(cls, payload: str) -> _Event[KeyT]:
        return _Event(**json.loads(payload))

    def as_json(self) -> str:
        return json.dumps(dataclasses.asdict(self))

    def get_key(self) -> KeyT:
        key_type = import_class(self.key_type)
        return key_type(*self.raw_key)


class StreamProducer:
    """Proxy stream producer interface.

    Note:
        The [`StreamProducer`][proxystore.stream.StreamProducer] can be
        used as a context manager.

        ```python
        with StreamProducer(...) as stream:
            for item in ...:
                stream.send(item)
        ```

    Note:
        The producer is only thread safe if the underlying
        [`Publisher`][proxystore.pubsub.protocols.Publisher] instance
        is thread safe.

    Args:
        store: [`Store`][proxystore.store.base.Store] instance used to store
            and communicate serialized objects in the stream.
        publisher: [`Publisher`][proxystore.pubsub.protocols.Publisher]
            instance used to publish new object in stream events.
    """

    def __init__(
        self,
        store: Store[Any],
        publisher: Publisher,
    ) -> None:
        self._store = store
        self._publisher = publisher

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self, *, store: bool = True, publisher: bool = True) -> None:
        """Close the producer.

        Warning:
            By default, this will also call `close()` on the
            [`Store`][proxystore.store.base.Store] and
            [`Publisher`][proxystore.pubsub.protocols.Publisher] interfaces.

        Args:
            store: Close the [`Store`][proxystore.store.base.Store] interface.
            publisher: Close the
                [`Publisher`][proxystore.pubsub.protocols.Publisher] interface.
        """
        if store:
            self._store.close()
        if publisher:
            self._publisher.close()

    def send(
        self,
        obj: Any,
        *,
        evict: bool = True,
        topic: str | None = None,
    ) -> None:
        """Send an item to the stream.

        This method (1) puts the object in the
        [`Store`][proxystore.store.base.Store] to get back an identifier key,
        (2) creates a new event using the key and additional metadata, and
        (3) publishes the event to the stream via the
        [`Publisher`][proxystore.pubsub.protocols.Publisher].

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
            obj: Object to send via the stream.
            evict: Evict the object from the
                [`Store`][proxystore.store.base.Store] once the object is
                consumed by a
                [`StreamConsumer`][proxystore.stream.StreamConsumer]. Set to
                `False` if a single object in the stream will be consumed by
                multiple consumers. Note that when set to `False`, data
                eviction must be handled manually.
            topic: Stream topic to publish to. `None` uses the default
                stream of the
                [`Publisher`][proxystore.pubsub.protocols.Publisher] instance.
        """
        key = self._store.put(obj)
        event: _Event[Any] = _Event.from_key(key, evict=evict)
        message = event.as_json().encode()
        self._publisher.send(message, topic=topic)


class StreamConsumer:
    """Proxy stream consumer interface.

    This interface acts as an iterator that will yield items from the stream
    until the stream is closed.

    Note:
        The [`StreamConsumer`][proxystore.stream.StreamConsumer] can be
        used as a context manager.

        ```python
        with StreamConsumer(...) as stream:
            for item in stream:
                ...
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

    Note:
        The consumer is only thread safe if the underlying
        [`Subscriber`][proxystore.pubsub.protocols.Subscriber] instance
        is thread safe.

    Args:
        store: [`Store`][proxystore.store.base.Store] instance used to
            retrieve serialized objects in the stream.
        subscriber: [`Subscriber`][proxystore.pubsub.protocols.Subscriber]
            instance to poll for new object in stream events.
    """

    def __init__(
        self,
        store: Store[Any],
        subscriber: Subscriber,
    ) -> None:
        self._store = store
        self._subscriber = subscriber

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
        return self

    def __next__(self) -> Proxy[Any]:
        return self.next()

    def close(self, *, store: bool = True, subscriber: bool = True) -> None:
        """Close the consumer.

        Warning:
            By default, this will also call `close()` on the
            [`Store`][proxystore.store.base.Store] and
            [`Publisher`][proxystore.pubsub.protocols.Publisher] interfaces.

        Args:
            store: Close the [`Store`][proxystore.store.base.Store] interface.
            subscriber: Close the
                [`Subscriber`][proxystore.pubsub.protocols.Subscriber]
                interface.
        """
        if store:
            self._store.close()
        if subscriber:
            self._subscriber.close()

    def next(self) -> Proxy[Any]:
        """Return a proxy of the next object in the stream.

        Raises:
            StopIteration: when the producer closes the stream.
        """
        message = next(self._subscriber)
        event: _Event[Any] = _Event.from_json(message.decode())
        proxy: Proxy[Any] = self._store.proxy_from_key(
            event.get_key(),
            evict=event.evict,
        )
        return proxy

from __future__ import annotations

import queue
import sys
from collections.abc import Mapping

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.stream.events import EventBatch
from proxystore.stream.protocols import EventPublisher
from proxystore.stream.protocols import EventSubscriber
from proxystore.stream.protocols import MessagePublisher
from proxystore.stream.protocols import MessageSubscriber
from proxystore.stream.shims.queue import QueuePublisher
from proxystore.stream.shims.queue import QueueSubscriber


class QueueEventPublisher:
    def __init__(
        self,
        queues: Mapping[str, queue.Queue[EventBatch]],
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        self._queues = queues
        self._block = block
        self._timeout = timeout

    def close(self) -> None:
        pass

    def send_events(self, events: EventBatch) -> None:
        assert events.topic in self._queues
        self._queues[events.topic].put(
            events,
            block=self._block,
            timeout=self._timeout,
        )


class QueueEventSubscriber:
    def __init__(
        self,
        queue: queue.Queue[EventBatch],
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        self._queue = queue
        self._block = block
        self._timeout = timeout

    def __iter__(self) -> Self:  # pragma: no cover
        return self

    def __next__(self) -> EventBatch:
        return self.next_events()

    def next_events(self) -> EventBatch:
        try:
            return self._queue.get(block=self._block, timeout=self._timeout)
        except ValueError:  # pragma: no cover
            raise StopIteration from None

    def close(self) -> None:
        pass


def create_event_pubsub_pair(
    topic: str | None = None,
) -> tuple[QueueEventPublisher, QueueEventSubscriber]:
    topic = 'default' if topic is None else topic
    queue_: queue.Queue[EventBatch] = queue.Queue()

    publisher = QueueEventPublisher({topic: queue_})
    subscriber = QueueEventSubscriber(queue_)

    assert isinstance(publisher, EventPublisher)
    assert isinstance(subscriber, EventSubscriber)

    return publisher, subscriber


def create_message_pubsub_pair(
    topic: str | None = None,
) -> tuple[QueuePublisher, QueueSubscriber]:
    topic = 'default' if topic is None else topic
    queue_: queue.Queue[bytes] = queue.Queue()

    publisher = QueuePublisher({topic: queue_})
    subscriber = QueueSubscriber(queue_)

    assert isinstance(publisher, MessagePublisher)
    assert isinstance(subscriber, MessageSubscriber)

    return publisher, subscriber

"""Python queue-based pub/sub implementation."""
from __future__ import annotations

import multiprocessing
import multiprocessing.queues
import queue
import sys
from typing import Mapping

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

_CLOSED_SENTINAL = b'<queue-publisher-closed-topic>'


class QueuePublisher:
    """Publisher built on Python queues.

    Warning:
        Each topic can only have one subscriber.

    Args:
        queues: Mapping of topic name to Python queue.
        default_topic: Default topic.
        block: Block until a free slot is available when sending a new message
            to the queue.
        timeout: Block at most `timeout` seconds.

    Raises:
        ValueError: if `default_topic` is not in the `queues` mapping.
    """

    def __init__(
        self,
        queues: Mapping[
            str,
            multiprocessing.Queue[bytes] | queue.Queue[bytes],
        ],
        default_topic: str,
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        if default_topic not in queues:
            raise ValueError(
                f'Default topic "{default_topic}" is not in the mapping'
                'of queues.',
            )

        self._queues = queues
        self._default_topic = default_topic
        self._block = block
        self._timeout = timeout

    def close(self) -> None:
        """Close this publisher and all topics associated with it.

        This will cause a [`StopIteration`][StopIteration] exception to be
        raised in any [`Subscriber`][proxystore.stream.protocols.Subscriber]
        instances that are currently iterating on new messages.
        """
        for q in self._queues.values():
            q.put(
                _CLOSED_SENTINAL,
                block=self._block,
                timeout=self._timeout,
            )

            if isinstance(q, multiprocessing.queues.Queue):
                q.close()

    def send(self, message: bytes, *, topic: str | None = None) -> None:
        """Publish a message to the stream.

        Args:
            message: Message as bytes to publish to the stream.
            topic: Stream topic to publish to. `None` uses the default topic.

        Raises:
            ValueError: if `topic` is not in the mapping of queues.
        """
        topic = topic if topic is not None else self._default_topic
        if topic not in self._queues:
            raise ValueError(f'Topic "{topic}" does not exist.')

        self._queues[topic].put(
            message,
            block=self._block,
            timeout=self._timeout,
        )


class QueueSubscriber:
    """Subscriber to a [`QueuePublisher`][proxystore.stream.shims.queue.QueuePublisher] topic.

    Warning:
        Each topic can only have one subscriber.

    Args:
        queue: Queue shared with the
            [`QueuePublisher`][proxystore.stream.shims.queue.QueuePublisher] to
            pull messages from.
        block: Block until the next message is available in the queue.
        timeout: Block at most `timeout` seconds.
    """  # noqa: E501

    def __init__(
        self,
        queue: multiprocessing.Queue[bytes] | queue.Queue[bytes],
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        self._queue = queue
        self._block = block
        self._timeout = timeout

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        try:
            message = self._queue.get(block=self._block, timeout=self._timeout)
        except ValueError:
            raise StopIteration from None

        if message == _CLOSED_SENTINAL:
            raise StopIteration

        return message

    def close(self) -> None:
        """Close this subscriber."""
        pass

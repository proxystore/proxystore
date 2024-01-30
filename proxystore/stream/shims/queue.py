"""Python queue-based pub/sub implementation.

Warning:
    This implementation is meant for streaming between Python threads
    within the same process, or between Python processes on the same machine.
    Each queue topic may only have one subscriber.
"""
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


class QueuePublisher:
    """Publisher built on Python queues.

    Warning:
        Each topic can only have one subscriber.

    Args:
        queues: Mapping of topic name to Python queue.
        block: Block until a free slot is available when sending a new message
            to the queue.
        timeout: Block at most `timeout` seconds.
    """

    def __init__(
        self,
        queues: Mapping[
            str,
            multiprocessing.Queue[bytes] | queue.Queue[bytes],
        ],
        *,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        self._queues = queues
        self._block = block
        self._timeout = timeout

    def close(self) -> None:
        """Close this publisher."""
        for q in self._queues.values():
            if isinstance(q, multiprocessing.queues.Queue):
                q.close()

    def send(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.

        Raises:
            ValueError: if a queue with the name `topic` does not exist.
        """
        if topic not in self._queues:
            raise ValueError(f'Unknown topic "{topic}".')
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

        return message

    def close(self) -> None:
        """Close this subscriber."""
        pass

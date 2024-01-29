"""ZeroMQ pub/sub interface."""
from __future__ import annotations

import sys
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import zmq

_CLOSED_SENTINAL = b'<queue-publisher-closed-topic>'


class ZeroMQPublisher:
    """ZeroMQ publisher interface.

    Args:
        address: Address to bind to. The full address bound to will be
            `'tcp://{address}:{port}'`.
        port: Port to bind to.
        topics: Sequence or set of all topics that might be published to.
        default_topic: Default topic to publish messages to. Must be contained
            in `topics`.

    Raises:
        ValueError: if `default_topic` is not in `topics`.
    """

    def __init__(
        self,
        address: str,
        port: int,
        *,
        topics: Sequence[str] | set[str] = ('default',),
        default_topic: str = 'default',
    ) -> None:
        if default_topic not in topics:
            raise ValueError(
                f'Default topic "{default_topic}" is not in the list of '
                f'all topic: {topics}.',
            )
        self._topics = topics
        self._default_topic = default_topic

        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PUB)
        self._socket.bind(f'tcp://{address}:{port}')

    def close(self, *, close_topics: bool = True) -> None:
        """Close this publisher.

        This will cause a [`StopIteration`][StopIteration] exception to be
        raised in any
        [`ZeroMQSubscriber`][proxystore.stream.shims.zmq.ZeroMQSubscriber]
        instances that are currently iterating on new messages from *any*
        of the topics registered with this publisher. This behavior can be
        altered by passing `close_topics=True`.

        Args:
            close_topics: Send an end-of-stream message to all topics
                associated with this publisher.
        """
        if close_topics:
            for topic in self._topics:
                self._socket.send_multipart(
                    (topic.encode(), _CLOSED_SENTINAL),
                )
        self._context.destroy()

    def send(self, message: bytes, *, topic: str | None = None) -> None:
        """Publish a message to the stream.

        Args:
            message: Message as bytes to publish to the stream.
            topic: Stream topic to publish to. `None` uses the default stream.

        Raises:
            ValueError: if `topic` is not in `topics` provided during
                initialization.
        """
        topic = topic if topic is not None else self._default_topic
        if topic not in self._topics:
            raise ValueError(f'Topic "{topic}" is unknown.')
        self._socket.send_multipart((topic.encode(), message))


class ZeroMQSubscriber:
    """ZeroMQ subscriber interface.

    The subscriber protocol is an iterable object which yields objects
    from the stream until the stream is closed.

    Args:
        address: Publisher address to connect to. The full address will be
            constructed as `'tcp://{address}:{port}'`.
        port: Publisher port to connect to.
        topic: Topic to subscribe to. The default `''` subscribes to all
            topics.
    """

    def __init__(self, address: str, port: int, *, topic: str = '') -> None:
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.connect(f'tcp://{address}:{port}')
        self._socket.setsockopt(zmq.SUBSCRIBE, topic.encode())

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        _, message = self._socket.recv_multipart()
        if message == _CLOSED_SENTINAL:
            raise StopIteration
        return message

    def close(self) -> None:
        """Close this subscriber."""
        self._context.destroy()

"""ZeroMQ pub/sub interface.

Note:
    Unlike some of the other shims that simply interface with a third-party
    message broker system, here the subscriber connects directly to
    the publisher. This means that if the publisher is not alive when creating
    the subscriber, the subscriber will fail.
"""
from __future__ import annotations

import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import zmq


class ZeroMQPublisher:
    """ZeroMQ publisher interface.

    Args:
        address: Address to bind to. The full address bound to will be
            `'tcp://{address}:{port}'`.
        port: Port to bind to.
    """

    def __init__(self, address: str, port: int) -> None:
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PUB)
        self._socket.bind(f'tcp://{address}:{port}')

    def close(self) -> None:
        """Close this publisher."""
        self._context.destroy()

    def send(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.
        """
        self._socket.send_multipart((topic.encode(), message))


class ZeroMQSubscriber:
    """ZeroMQ subscriber interface.

    This subscriber is an iterable object which yields [`bytes`][bytes]
    messages indefinitely from the stream while connected to a publisher.

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
        return message

    def close(self) -> None:
        """Close this subscriber."""
        self._context.destroy()

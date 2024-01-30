"""Publisher and subscriber protocol definitions.

The [`Publisher`][proxystore.stream.protocols.Publisher] and
[`Subscriber`][proxystore.stream.protocols.Subscriber] are
[`Protocols`][typing.Protocol] which define the publisher and subscriber
interfaces to a pub/sub-like messaging system.

In general, these protocols do not enforce any other implementation details
besides the interface. For example, implementations could choose to support
any producer-to-consumer configurations (e.g., 1:1, 1:N, N:N).
A set of shims implementing these protocols for third-party message brokers
are provided in [`proxystore.stream.shims`][proxystore.stream.shims].
"""
from __future__ import annotations

import sys
from typing import Protocol
from typing import runtime_checkable

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


@runtime_checkable
class Publisher(Protocol):
    """Publisher interface to message stream."""

    def close(self) -> None:
        """Close this publisher."""
        ...

    def send(self, topic: str, message: bytes) -> None:
        """Publish a message to the stream.

        Args:
            topic: Stream topic to publish message to.
            message: Message as bytes to publish to the stream.
        """
        ...


@runtime_checkable
class Subscriber(Protocol):
    """Subscriber interface to message stream.

    The subscriber protocol is an iterable object which yields objects
    from the stream until the stream is closed.
    """

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> bytes:
        ...

    def close(self) -> None:
        """Close this subscriber."""
        ...

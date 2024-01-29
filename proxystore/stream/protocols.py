"""Publisher and subscriber protocol definitions.

The [`Publisher`][proxystore.stream.protocols.Publisher] and
[`Subscriber`][proxystore.stream.protocols.Subscriber] are
[`Protocols`][typing.Protocol] which define the publisher and subscriber
interfaces to a pub/sub-like messaging system.

In general, these protocols do not enforce any other implementation details
besides the interface. For example, implementations could choose to support
any producer-to-consumer configurations (e.g., 1:1, 1:N, N:N).
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
        """Close this publisher.

        This will cause a [`StopIteration`][StopIteration] exception to be
        raised in any [`Subscriber`][proxystore.stream.protocols.Subscriber]
        instances that are currently iterating on new messages.
        """
        ...

    def send(self, message: bytes, *, topic: str | None = None) -> None:
        """Publish a message to the stream.

        Args:
            message: Message as bytes to publish to the stream.
            topic: Stream topic to publish to. `None` uses the default stream.
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

"""Pub/sub protocols."""
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
        raised in any [`Subscriber`][proxystore.pubsub.protocols.Subscriber]
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
    """Subscriber interface to message stream."""

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> bytes:
        ...

    def close(self) -> None:
        """Close this subscriber."""
        ...

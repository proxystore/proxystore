"""Relay client interface protocol."""
from __future__ import annotations

import uuid
from typing import Protocol
from typing import runtime_checkable

from proxystore.p2p.messages import Message
from proxystore.p2p.messages import MessageDecodeError  # noqa: F401


@runtime_checkable
class RelayClient(Protocol):
    """Client protocol for interfacing with a relay server."""

    @property
    def name(self) -> str:
        """Name of client as registered with relay server."""
        ...

    @property
    def uuid(self) -> uuid.UUID:
        """UUID of client as registered with relay server."""
        ...

    async def close(self) -> None:
        """Close connection to the relay server."""
        ...

    async def connect(self) -> None:
        """Connect to the relay server."""

    async def recv(self) -> Message:
        """Receive the next message from the relay server.

        Returns:
            The message received from the relay server.

        Raises:
            MessageDecodeError: If the message received cannot be decoded into
                the appropriate message type.
        """
        ...

    async def send(self, message: Message) -> None:
        """Send a message to the relay server.

        Args:
            message: The message to send to the relay server.
        """
        ...

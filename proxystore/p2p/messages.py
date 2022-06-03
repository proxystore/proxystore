"""Message types for peer-to-peer communication."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any


@dataclass
class BaseMessage:
    """Base message for peer-to-peer networking."""

    pass


@dataclass
class ServerError(BaseMessage):
    """Message returned by signaling server on error."""

    message: str


@dataclass
class PeerRegistrationRequest(BaseMessage):
    """Register with signaling server as peer."""

    name: str
    uuid: uuid.UUID | None = None


@dataclass
class PeerRegistrationResponse(BaseMessage):
    """Peer registration response from signaling server."""

    uuid: uuid.UUID
    error: Exception | None = None


@dataclass
class PeerConnectionMessage(BaseMessage):
    """Message used in establishing a peer-to-peer connection."""

    source_uuid: uuid.UUID
    source_name: str
    peer_uuid: uuid.UUID
    message: str | None = None
    error: Exception | None = None


@dataclass
class PeerMessage:
    """Message sent between peers."""

    message_id: str
    source_uuid: uuid.UUID
    peer_uuid: uuid.UUID
    message: Any


@dataclass
class PeerResponse:
    """Response message from peer."""

    message_id: str
    source_uuid: uuid.UUID
    peer_uuid: uuid.UUID
    message: Any = None
    error: Exception | None = None

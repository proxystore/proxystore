"""Message types."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BaseMessage:
    """Base message."""

    pass


@dataclass
class EndpointRegistrationRequest(BaseMessage):
    """Endpoint registration request.

    Message sent by an endpoint to the signaling server to register in order
    to begin establishing peer-to-peer connections.
    """

    name: str
    uuid: str


@dataclass
class EndpointRegistrationSuccess(BaseMessage):
    """Endpoint registration success.

    Reply sent by the signaling server upon successful registration.
    """

    name: str
    uuid: str


@dataclass
class P2PConnectionBaseMessage(BaseMessage):
    """Base message for messages used for establishing P2P connections."""

    source_uuid: str
    target_uuid: str


@dataclass
class P2PConnectionMessage(P2PConnectionBaseMessage):
    """Messages used for establishing P2P connections."""

    message: str


@dataclass
class P2PConnectionError(P2PConnectionBaseMessage):
    """Error encountered by signaling server when brokering messages."""

    error: str | Exception


@dataclass
class P2PDataTransfer(BaseMessage):
    """Base message for data transfers between peers."""


@dataclass
class P2PDataTransferRequest(BaseMessage):
    """Base message for data transfers between peers."""

    key: str


@dataclass
class P2PDataTransferResponse(BaseMessage):
    """Base message for data transfers between peers."""

    success: bool
    data: Any

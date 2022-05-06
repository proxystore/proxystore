"""Exception types for the Signaling Server."""
from __future__ import annotations


class PeerConnectionTimeout(Exception):
    """Timeout waiting on peer to peer connection to establish."""

    pass


class PeerRegistrationError(Exception):
    """Error when establishing peer connection."""

    pass


class PeerUnknownError(Exception):
    """Error when signaling server does not know a peer."""

    pass

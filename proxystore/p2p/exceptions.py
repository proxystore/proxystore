"""Exception types for the Signaling Server."""
from __future__ import annotations


class PeerConnectionError(Exception):
    """Error connecting to peer."""

    pass


class PeerConnectionTimeout(PeerConnectionError):
    """Timeout waiting on peer to peer connection to establish."""

    pass


class PeerRegistrationError(Exception):
    """Error when establishing peer connection."""

    pass

"""Exception types for peering errors."""
from __future__ import annotations


class PeerConnectionError(Exception):
    """Error connecting to peer."""

    pass


class PeerConnectionTimeoutError(PeerConnectionError):
    """Timeout waiting on peer to peer connection to establish."""

    pass

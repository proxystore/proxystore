"""Exception types for peering errors."""

from __future__ import annotations


class PeerConnectionError(Exception):
    """Error connecting to or communicating with a peer."""


class PeerConnectionTimeoutError(PeerConnectionError):
    """Timeout waiting on a connection to a peer to be established."""


class PeerNotAllowedError(PeerConnectionError):
    """Peer is not in the allowlist of this endpoint or refused this endpoint.

    Two endpoints can only communicate if each endpoint has the other in its
    allowlist.
    """

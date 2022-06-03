"""Endpoint exceptions."""
from __future__ import annotations


class PeeringNotAvailableError(Exception):
    """Exception when a peer request is made but peering is not available."""

    pass


class PeerRequestError(Exception):
    """Exception raised when a request to a peer fails."""

    pass

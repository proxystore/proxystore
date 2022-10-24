"""Endpoint exceptions."""
from __future__ import annotations


class FileDumpNotAvailableError(Exception):
    """Error raised when dumping objects to file is not available."""

    pass


class ObjectSizeExceededError(Exception):
    """Exception raised when an object exceeds the max allowable size."""

    pass


class PeeringNotAvailableError(Exception):
    """Exception when a peer request is made but peering is not available."""

    pass


class PeerRequestError(Exception):
    """Exception raised when a request to a peer fails."""

    pass

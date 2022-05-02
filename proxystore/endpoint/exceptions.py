"""Endpoint exceptions."""
from __future__ import annotations


class PeeringNotAvailableError(Exception):
    """Exception when a peer request is made but peering is not available."""

    pass

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


class EndpointClientError(Exception):
    """Base exception for errors communicating with an endpoint."""

    pass


class EndpointAuthError(EndpointClientError):
    """Exception raised when the client or endpoint fails authentication."""

    pass


class EndpointProtocolError(EndpointClientError):
    """Exception raised for malformed or incompatible protocol messages."""

    pass


class EndpointRequestError(EndpointClientError):
    """Exception raised when the endpoint returns an error for a request."""

    pass

"""Endpoint exceptions."""

from __future__ import annotations


class FileDumpNotAvailableError(Exception):
    """Error raised when dumping objects to file is not available."""

    pass


class PeeringNotAvailableError(Exception):
    """Exception when a peer request is made but peering is not available."""

    pass


class PeerRequestError(Exception):
    """Exception raised when a request to a peer fails."""

    pass


class EndpointError(Exception):
    """Base exception for errors communicating with an endpoint."""

    pass


class EndpointAuthError(EndpointError):
    """Exception raised when the client or endpoint fails authentication."""

    pass


class EndpointConnectionError(EndpointError):
    """Exception raised when the connection to an endpoint is closed or lost.

    This is only raised by a client when the connection closes unexpectedly
    (e.g., because the endpoint was stopped).
    """

    pass


class EndpointNotRunningError(EndpointError):
    """Exception raised when connecting to an endpoint that is not running.

    This is raised when the endpoint has never been started (i.e., its
    configuration has no host) or its credential files do not exist.
    """

    pass


class EndpointProtocolError(EndpointError):
    """Exception raised for malformed or incompatible protocol messages."""

    pass


class EndpointRequestError(EndpointError):
    """Exception raised when the endpoint returns an error for a request."""

    pass


class ObjectSizeExceededError(EndpointRequestError):
    """Exception raised when an object exceeds the max allowable size."""

    pass

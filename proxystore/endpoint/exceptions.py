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


class EndpointConnectorError(EndpointError):
    """Exception raised when a request by an endpoint connector fails.

    Raised by the
    [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector]
    with the error that caused the request to fail as the cause.
    """

    pass


class EndpointNotFoundError(EndpointError):
    """Exception raised when connecting to an endpoint that does not exist.

    This is raised when the endpoint's directory does not exist (e.g.,
    because the endpoint was never configured or the name is misspelled).
    """

    pass


class EndpointNotRunningError(EndpointError):
    """Exception raised when connecting to an endpoint that is not running.

    This is raised when the endpoint's connection file does not exist (i.e.,
    the endpoint has not been started or has stopped) or when the connection
    to the endpoint's address is refused.
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

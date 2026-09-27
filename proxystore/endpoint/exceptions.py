"""Endpoint exceptions.

All exceptions raised by endpoints, clients, and peering derive from
[`EndpointError`][proxystore.endpoint.exceptions.EndpointError]:

```
EndpointError
├── EndpointNotFoundError (also FileNotFoundError)
├── EndpointExistsError (also FileExistsError)
├── EndpointConfigError (also ValueError)
│   └── PeerExistsError
├── EndpointRunningError
├── EndpointNotRunningError
├── EndpointAuthError
├── EndpointConnectionError
├── EndpointProtocolError
├── EndpointConnectorError
├── EndpointRequestError
│   └── ObjectSizeExceededError
└── PeerError
    ├── PeeringNotAvailableError
    ├── PeerRequestError
    └── PeerConnectionError
        ├── PeerConnectionTimeoutError
        └── PeerNotAllowedError
```

Some exceptions also derive from a built-in exception so they can be caught
as either (e.g., an invalid configuration is also a `ValueError`).
"""

from __future__ import annotations


class EndpointError(Exception):
    """Base exception for all endpoint errors."""


class EndpointNotFoundError(EndpointError, FileNotFoundError):
    """Exception raised when an endpoint does not exist.

    This is raised when the endpoint's directory or configuration does not
    exist (e.g., because the endpoint was never configured or the name is
    misspelled).
    """


class EndpointExistsError(EndpointError, FileExistsError):
    """Exception raised when creating an endpoint that already exists."""


class EndpointConfigError(EndpointError, ValueError):
    """Exception raised when a file in an endpoint directory is invalid.

    This includes the configuration, secret key, peers, and connection files
    (e.g., because a file is malformed, has an unsupported format version, or
    the secret key does not match the configuration).
    """


class PeerExistsError(EndpointConfigError):
    """Exception raised when adding a peer whose name is already used."""


class EndpointRunningError(EndpointError):
    """Exception raised when an operation requires a stopped endpoint.

    For example, when starting or removing an endpoint that is already
    running on this or another host.
    """


class EndpointNotRunningError(EndpointError):
    """Exception raised when connecting to an endpoint that is not running.

    This is raised when the endpoint's connection file does not exist (i.e.,
    the endpoint has not been started or has stopped) or when the connection
    to the endpoint's address is refused.
    """


class EndpointAuthError(EndpointError):
    """Exception raised when the client or endpoint fails authentication."""


class EndpointConnectionError(EndpointError):
    """Exception raised when the connection to an endpoint is closed or lost.

    This is only raised by a client when the connection closes unexpectedly
    (e.g., because the endpoint was stopped).
    """


class EndpointProtocolError(EndpointError):
    """Exception raised for malformed or incompatible protocol messages."""


class EndpointConnectorError(EndpointError):
    """Exception raised when a request by an endpoint connector fails.

    Raised by the
    [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector]
    with the error that caused the request to fail as the cause.
    """


class EndpointRequestError(EndpointError):
    """Exception raised when the endpoint returns an error for a request."""


class ObjectSizeExceededError(EndpointRequestError):
    """Exception raised when an object exceeds the max allowable size."""


class PeerError(EndpointError):
    """Base exception for errors communicating with a peer endpoint."""


class PeeringNotAvailableError(PeerError):
    """Exception raised when a peer request is made but peering is disabled."""


class PeerRequestError(PeerError):
    """Exception raised when a peer endpoint returns an error for a request."""


class PeerConnectionError(PeerError):
    """Exception raised when connecting to or communicating with a peer."""


class PeerConnectionTimeoutError(PeerConnectionError):
    """Exception raised when connecting to a peer times out."""


class PeerNotAllowedError(PeerConnectionError):
    """Exception raised when a peer is not allowed.

    Two endpoints can only communicate if each endpoint has the other in its
    allowlist of peers. This is raised if the peer is not in the allowlist of
    this endpoint or the peer refused the connection because this endpoint
    is not in its allowlist.
    """

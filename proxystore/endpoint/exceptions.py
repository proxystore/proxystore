"""Endpoint exceptions.

All exceptions raised by endpoints, clients, and peering derive from
[`EndpointError`][proxystore.endpoint.exceptions.EndpointError]:

```
EndpointError
├── EndpointNotFoundError (also FileNotFoundError)
├── EndpointExistsError (also FileExistsError)
├── EndpointConfigError (also ValueError)
│   ├── PeerExistsError
│   └── PeerNotFoundError
├── EndpointRunningError
├── EndpointNotRunningError
├── EndpointConnectionError
├── EndpointAuthError
├── EndpointProtocolError
├── EndpointConnectorError
└── EndpointRequestError
    ├── ObjectSizeExceededError
    └── PeerError
        ├── PeeringDisabledError
        ├── PeerNotAllowedError
        └── PeerUnavailableError
            └── PeerConnectionTimeoutError
```

Some exceptions also derive from a built-in exception so they can be caught
as either (e.g., an invalid configuration is also a `ValueError`).

A request that fails on an endpoint raises the same type of exception in
the client (see
[`raise_for_status()`][proxystore.endpoint.protocol.raise_for_status]). For
example, a request forwarded to a peer which is not in the allowlist of the
endpoint raises a
[`PeerNotAllowedError`][proxystore.endpoint.exceptions.PeerNotAllowedError]
in the endpoint which is returned to the client as the
[`PEER_NOT_ALLOWED`][proxystore.endpoint.protocol.Status.PEER_NOT_ALLOWED]
status and raised again by the client.
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
    the secret key does not match the configuration), and invalid changes
    to those files (e.g., adding a peer with an invalid name).
    """


class PeerExistsError(EndpointConfigError):
    """Exception raised when adding a peer whose name is already used."""


class PeerNotFoundError(EndpointConfigError):
    """Exception raised when removing a peer that does not exist."""


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


class EndpointConnectionError(EndpointError):
    """Exception raised when the connection to an endpoint is closed or lost.

    This is only raised by a client when the connection closes unexpectedly
    (e.g., because the endpoint was stopped).
    """


class EndpointAuthError(EndpointError):
    """Exception raised when the client or endpoint fails authentication."""


class EndpointProtocolError(EndpointError):
    """Exception raised for malformed or incompatible protocol messages."""


class EndpointConnectorError(EndpointError):
    """Exception raised when a request by an endpoint connector fails.

    Raised by the
    [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector]
    with the error that caused the request to fail as the cause.
    """


class EndpointRequestError(EndpointError):
    """Exception raised when an endpoint fails to perform a request.

    Subclasses indicate the reason the request failed. This base class is
    raised for unexpected errors (e.g., a failure in the storage of the
    endpoint).
    """


class ObjectSizeExceededError(EndpointRequestError):
    """Exception raised when an object exceeds the max allowable size."""


class PeerError(EndpointRequestError):
    """Base exception for errors forwarding a request to a peer endpoint."""


class PeeringDisabledError(PeerError):
    """Exception raised when a request targets a peer but peering is disabled.

    Enable peering in the configuration of the endpoint to forward requests
    to peers.
    """


class PeerNotAllowedError(PeerError):
    """Exception raised when a peer is not allowed.

    Two endpoints can only communicate if each endpoint has the other in its
    allowlist of peers. This is raised if the peer is not in the allowlist of
    this endpoint or the peer refused the connection because this endpoint
    is not in its allowlist.
    """


class PeerUnavailableError(PeerError):
    """Exception raised when connecting to or communicating with a peer fails.

    The peer may not be running, may be unreachable from this endpoint, or
    the connection to the peer was lost. Retrying the request later may
    succeed.
    """


class PeerConnectionTimeoutError(PeerUnavailableError):
    """Exception raised when connecting to a peer times out.

    Clients receive a
    [`PeerUnavailableError`][proxystore.endpoint.exceptions.PeerUnavailableError]
    rather than this more specific exception.
    """

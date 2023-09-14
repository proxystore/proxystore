"""Exception types raised by relay clients and servers."""
from __future__ import annotations


class RelayClientError(Exception):
    """Base exception type for exceptions raised by relay clients."""

    pass


class RelayNotConnectedError(RelayClientError):
    """Exception raised if a client is not connected to a relay server."""

    pass


class RelayRegistrationError(RelayClientError):
    """Exception raised by client if unable to register with relay server."""

    pass


class RelayServerError(Exception):
    """Base exception type for exceptions raised by relay server."""

    pass


class BadRequestError(RelayServerError):
    """A runtime exception indicating a bad client request."""

    pass


class UnknownPeerRequestedError(BadRequestError):
    """An exception indicating a client wants to peer with an unknown peer."""

    pass

"""Exception types raised by relay clients and servers."""

from __future__ import annotations


class RelayClientError(Exception):
    """Base exception type for exceptions raised by relay clients."""


class RelayNotConnectedError(RelayClientError):
    """Exception raised if a client is not connected to a relay server."""


class RelayRegistrationError(RelayClientError):
    """Exception raised by client if unable to register with relay server."""


class RelayServerError(Exception):
    """Base exception type for exceptions raised by relay server."""


class BadRequestError(RelayServerError):
    """A runtime exception indicating a bad client request."""


class ForbiddenError(RelayServerError):
    """Client does not have correct permissions after authentication."""


class InternalServerError(RelayServerError):
    """Server encountered an unexpected condition."""


class UnauthorizedError(RelayServerError):
    """Client is missing authentication tokens."""

"""Exception types for the Signaling Server."""
from __future__ import annotations


class ServerException(Exception):
    """Base exception type for exceptions returned by the signaling server."""

    pass


class UnknownMessageType(ServerException):
    """Exception when the signaling server gets unknown message type."""

    pass


class EndpointNotRegisteredError(ServerException):
    """Exception when an endpoint attempts connections before registering."""

    pass


class EndpointRegistrationError(ServerException):
    """Exception returned by server if registration request fails."""

    pass

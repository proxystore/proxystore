"""Authenticate users from request headers."""
from __future__ import annotations

import dataclasses
import uuid
from typing import Any
from typing import Mapping
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

import globus_sdk

from proxystore.globus.client import get_confidential_app_auth_client
from proxystore.globus.scopes import ProxyStoreRelayScopes
from proxystore.p2p.relay.config import RelayAuthConfig
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError

UserT = TypeVar('UserT', covariant=True)
"""Auth user generic type."""


@runtime_checkable
class Authenticator(Protocol[UserT]):
    """Authenticate users from request headers."""

    def authenticate_user(self, headers: Mapping[str, str]) -> UserT:
        """Authenticate user from request headers.

        Args:
            headers: Request headers.

        Returns:
            User representation on authentication success.

        Raises:
            ForbiddenError: user is authenticated but is missing permissions
                or accessing forbidden resources.
            UnauthorizedError: user authentication fails.
        """
        ...


class NullUser:
    """Null user that is always equal to another null user instance."""

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self))

    def __repr__(self) -> str:
        return 'NullUser()'


class NullAuthenticator:
    """Authenticator that implements no authentication."""

    def authenticate_user(self, headers: Mapping[str, str]) -> NullUser:
        """Authenticate user from request headers.

        Args:
            headers: Request headers.

        Returns:
            Null user regardless of provided headers.
        """
        return NullUser()


@dataclasses.dataclass(frozen=True, eq=False)
class GlobusUser:
    """Globus Auth user information.

    Fields are retrieved via the
    [token introspection API](https://docs.globus.org/api/auth/reference/#token-introspec).

    Attributes:
        username: Identity username.
        client_id: The Globus Auth issues client id of the client to which
            the introspected token was issued.
        email: Email address associated with the effective identity of the
            introspected token. May be `None` if the user restricts their
            identity visibility.
        display_name: Display name associated with the effective identity of
            the introspected token. May be `None` if the user restricts their
            identity visibility.
    """

    username: str
    client_id: uuid.UUID
    email: str | None = None
    display_name: str | None = None

    def __eq__(self, other: object) -> bool:
        """Check equality using only Globus Auth client ID."""
        if isinstance(other, GlobusUser):
            return self.client_id == other.client_id
        else:
            return False


class GlobusAuthenticator:
    """Globus Auth authorizer.

    Args:
        client_id: Globus application client ID. If either `client_id`
            or `client_secret` is `None`, the values will be read from the
            environment variables as described in
            [`get_confidential_app_auth_client`][proxystore.globus.client.get_confidential_app_auth_client].
            Ignored if `auth_client` is provided.
        client_secret: Globus application client secret. See `client_id` for
            details. Ignored if `auth_client` is provided.
        audience: Intended audience of the token. This should typically be
            the resource server of the the token was issued for. E.g.,
            the UUID of the ProxyStore Relay Server application.
        auth_client: Optional confidential application authentication client
            which is used for introspecting client tokens.
    """

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        *,
        audience: str = ProxyStoreRelayScopes.resource_server,
        auth_client: globus_sdk.ConfidentialAppAuthClient | None = None,
    ) -> None:
        self.auth_client = (
            get_confidential_app_auth_client(client_id, client_secret)
            if auth_client is None
            else auth_client
        )
        self.audience = audience

    def authenticate_user(self, headers: Mapping[str, str]) -> GlobusUser:
        """Authenticate a Globus Auth user from request header.

        This follows from the [Globus Sample Data Portal](https://github.com/globus/globus-sample-data-portal/blob/30e30cd418ee9b103e04916e19deb9902d3aafd8/service/decorators.py)
        example.

        Args:
            headers: Request headers to extract tokens from.

        Returns:
            Globus Auth identity returned via \
            [token introspection](https://docs.globus.org/api/auth/reference/#token-introspect).

        Raises:
            UnauthorizedError: if the authorization header is missing or
                the header is malformed.
            ForbiddenError: if the tokens have expired or been revoked.
            ForbiddenError: if `audience` is not included in the token's
                audience.
        """
        token = get_token_from_headers(headers)
        token_meta = self.auth_client.oauth2_token_introspect(token)

        if not token_meta.get('active'):
            raise ForbiddenError('Token is expired or has been revoked.')

        if self.audience is not None and self.audience not in token_meta.get(
            'aud',
            [],
        ):
            raise ForbiddenError(
                f'Token audience does not include "{self.audience}". This '
                'could result in a confused deputy attack. Ensure the correct '
                'scopes are requested when the token is created.',
            )

        return GlobusUser(
            username=token_meta.get('username'),
            client_id=uuid.UUID(token_meta.get('client_id')),
            email=token_meta.get('email', None),
            display_name=token_meta.get('name', None),
        )


def get_authenticator(config: RelayAuthConfig) -> Authenticator[Any]:
    """Create an authenticator from a configuration.

    Args:
        config: Configuration.

    Returns:
        Authenticator.

    Raises:
        ValueError: if the authentication method in the config is unknown.
    """
    if config.method is None:
        return NullAuthenticator()
    elif config.method == 'globus':
        return GlobusAuthenticator(**config.kwargs)
    else:
        raise ValueError(f'Unknown authentication method "{config.method}."')


def get_token_from_headers(headers: Mapping[str, str]) -> str:
    """Extract token from websockets headers.

    The header is expected to have the format `Authorization: Bearer <TOKEN>`.

    Args:
         headers: Request headers to extract tokens from.

    Returns:
        String token.

    Raises:
        UnauthorizedError: if the authorization header is missing.
        UnauthorizedError: if the authorization header is malformed.
    """
    if 'Authorization' not in headers:
        raise UnauthorizedError(
            'Request headers are missing authorization header.',
        )

    auth_header_parts = headers['Authorization'].split(' ')

    if len(auth_header_parts) != 2 or auth_header_parts[0] != 'Bearer':
        raise UnauthorizedError(
            'Bearer token in authorization header is malformed.',
        )

    return auth_header_parts[1]

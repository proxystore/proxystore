"""Authenticate users from request headers."""
from __future__ import annotations

import dataclasses
import uuid
from typing import Mapping
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

import globus_sdk

from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError

PROXYSTORE_RESOURCE_SERVER_NAME = 'ProxyStore'

User = TypeVar('User', covariant=True)


@runtime_checkable
class Authenticator(Protocol[User]):
    """Authenticate users from request headers."""

    def authenticate_user(self, headers: Mapping[str, str]) -> User:
        """Authenticate user from request headers.

        Args:
            headers: Request headers.

        Returns:
            User representation on authentication success.

        Raises:
            ForbiddenError
            UnauthorizedError
        """
        ...


class NullUser:
    """Null user that is always equal to another null user instance."""

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self))


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
        auth_client: Confidential application authentication client which is
            used for introspecting client tokens.
        audience: Intended audience of the token.
    """

    def __init__(
        self,
        auth_client: globus_sdk.ConfidentialAppAuthClient,
        audience: str = PROXYSTORE_RESOURCE_SERVER_NAME,
    ) -> None:
        self._auth_client = auth_client
        self._audience = audience

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
            ForbiddenError: if the identity set of the tokens does not match.
        """
        token = get_token_from_headers(headers)
        token_meta = self._auth_client.oauth2_token_introspect(token)

        if not token_meta.get('active'):
            raise ForbiddenError('Token is expired or has been revoked.')

        if (
            self._audience is not None
            and self._audience not in token_meta.get('aud', [])
        ):
            raise ForbiddenError(
                f'Token audience does not include "{self._audience}". This '
                'could result in a confused deputy attack. Ensure the correct '
                'scopes are requested when the token is created.',
            )

        if self._auth_client.client_id != token_meta.get('sub'):
            raise ForbiddenError(
                'The identity set of the token does not match this application '
                'client ID.',
            )

        return GlobusUser(
            username=token_meta.get('username'),
            client_id=uuid.UUID(token_meta.get('client_id')),
            email=token_meta.get('email', None),
            display_name=token_meta.get('name', None),
        )


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

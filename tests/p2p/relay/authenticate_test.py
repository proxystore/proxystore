from __future__ import annotations

import uuid
from typing import Any
from unittest import mock

import pytest
import websockets
import websockets.datastructures

from proxystore.p2p.relay.authenticate import get_authenticator
from proxystore.p2p.relay.authenticate import get_token_from_headers
from proxystore.p2p.relay.authenticate import GlobusAuthenticator
from proxystore.p2p.relay.authenticate import GlobusUser
from proxystore.p2p.relay.authenticate import NullAuthenticator
from proxystore.p2p.relay.authenticate import NullUser
from proxystore.p2p.relay.config import RelayAuthConfig
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError


def test_null_authenticator() -> None:
    user1 = NullAuthenticator().authenticate_user({})
    user2 = NullAuthenticator().authenticate_user({'Authorization': 'token'})
    assert user1 == user2
    assert isinstance(NullAuthenticator().authenticate_user({}), NullUser)


def test_globus_user_equality() -> None:
    user1 = GlobusUser('user1', uuid.uuid4())
    user2 = GlobusUser('user2', uuid.uuid4())
    assert user1 != user2

    user2 = GlobusUser('user1', user1.sub)
    assert user1 == user2

    assert user1 != object()


def test_authenticate_user_with_token() -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), '')

    token_meta: dict[str, Any] = {
        'active': True,
        'aud': [authenticator.audience],
        'sub': str(uuid.uuid4()),
        'username': 'username',
        'client_id': str(uuid.uuid4()),
        'email': 'username@example.com',
        'name': 'User Name',
    }

    with mock.patch.object(
        authenticator.auth_client,
        'oauth2_token_introspect',
        return_value=token_meta,
    ):
        user = authenticator.authenticate_user(
            {'Authorization': 'Bearer <TOKEN>'},
        )

    assert user == GlobusUser(
        username=token_meta['username'],
        sub=uuid.UUID(token_meta['sub']),
        email=token_meta['email'],
        display_name=token_meta['name'],
    )


def test_authenticate_user_with_token_expired_token() -> None:
    authenticator = GlobusAuthenticator(str(uuid.uuid4()), '')
    with (
        mock.patch.object(
            authenticator.auth_client,
            'oauth2_token_introspect',
            return_value={'active': False},
        ),
        pytest.raises(
            ForbiddenError,
            match='Token is expired or has been revoked.',
        ),
    ):
        authenticator.authenticate_user({'Authorization': 'Bearer <TOKEN>'})


def test_authenticate_user_with_token_wrong_audience() -> None:
    authenticator = GlobusAuthenticator(
        str(uuid.uuid4()),
        '',
        audience='audience',
    )
    with (
        mock.patch.object(
            authenticator.auth_client,
            'oauth2_token_introspect',
            return_value={'active': True},
        ),
        pytest.raises(
            ForbiddenError,
            match='Token audience does not include "audience"',
        ),
    ):
        authenticator.authenticate_user({'Authorization': 'Bearer <TOKEN>'})


def test_get_authenticator() -> None:
    config = RelayAuthConfig()
    authenticator = get_authenticator(config)
    assert isinstance(authenticator, NullAuthenticator)

    config = RelayAuthConfig(
        method='globus',
        kwargs={
            'audience': 'test',
            'client_id': str(uuid.uuid4()),
            'client_secret': 'test',
        },
    )
    authenticator = get_authenticator(config)
    assert isinstance(authenticator, GlobusAuthenticator)
    assert authenticator.audience == 'test'


def test_get_authenticator_unknown() -> None:
    config = RelayAuthConfig(method='globus')
    # Modify attribute after construction to avoid Pydantic checking string
    # literal type.
    config.method = 'test'  # type: ignore[assignment]
    with pytest.raises(ValueError, match='Unknown authentication method'):
        get_authenticator(config)


def test_get_token_from_headers() -> None:
    headers = {'Authorization': 'Bearer <TOKEN>'}
    assert get_token_from_headers(headers) == '<TOKEN>'


def test_get_token_from_headers_websocket_headers() -> None:
    options = {'Authorization': 'Bearer <TOKEN>'}
    headers = websockets.datastructures.Headers(**options)
    assert get_token_from_headers(headers) == '<TOKEN>'


def test_get_token_from_headers_missing() -> None:
    with pytest.raises(
        UnauthorizedError,
        match='Request headers are missing authorization header.',
    ):
        get_token_from_headers({})


def test_get_token_from_headers_malformed() -> None:
    with pytest.raises(
        UnauthorizedError,
        match='Bearer token in authorization header is malformed.',
    ):
        get_token_from_headers({'Authorization': '<TOKEN>'})

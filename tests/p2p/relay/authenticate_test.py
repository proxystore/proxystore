from __future__ import annotations

import uuid
from typing import Any
from unittest import mock

import globus_sdk
import pytest
import websockets

from proxystore.p2p.relay.authenticate import get_token_from_headers
from proxystore.p2p.relay.authenticate import GlobusAuthenticator
from proxystore.p2p.relay.authenticate import GlobusUser
from proxystore.p2p.relay.authenticate import NullAuthenticator
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError


def test_null_authenticator() -> None:
    user1 = NullAuthenticator().authenticate_user({})
    user2 = NullAuthenticator().authenticate_user({'Authorization': 'token'})
    assert user1 == user2
    assert isinstance(NullAuthenticator().authenticate_user({}), str)


def test_globus_user_equality() -> None:
    user1 = GlobusUser('username', uuid.uuid4())
    user2 = GlobusUser('username', uuid.uuid4())
    assert user1 != user2

    user2 = GlobusUser('different-username', user1.client_id)
    assert user1 == user2

    assert user1 != object()


def test_authenticate_user_with_token() -> None:
    auth_client = globus_sdk.ConfidentialAppAuthClient(str(uuid.uuid4()), '')
    authenticator = GlobusAuthenticator(auth_client)

    token_meta: dict[str, Any] = {
        'active': True,
        'aud': [authenticator._audience],
        'sub': auth_client.client_id,
        'username': 'username',
        'client_id': str(uuid.uuid4()),
        'email': 'username@example.com',
        'name': 'User Name',
    }

    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value=token_meta,
    ):
        user = authenticator.authenticate_user(
            {'Authorization': 'Bearer <TOKEN>'},
        )

    assert user == GlobusUser(
        username=token_meta['username'],
        client_id=uuid.UUID(token_meta['client_id']),
        email=token_meta['email'],
        display_name=token_meta['name'],
    )


def test_authenticate_user_with_token_expired_token() -> None:
    auth_client = globus_sdk.ConfidentialAppAuthClient(str(uuid.uuid4()), '')
    authenticator = GlobusAuthenticator(auth_client)
    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value={'active': False},
    ), pytest.raises(
        ForbiddenError,
        match='Token is expired or has been revoked.',
    ):
        authenticator.authenticate_user({'Authorization': 'Bearer <TOKEN>'})


def test_authenticate_user_with_token_wrong_audience() -> None:
    auth_client = globus_sdk.ConfidentialAppAuthClient(str(uuid.uuid4()), '')
    authenticator = GlobusAuthenticator(auth_client, audience='audience')
    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value={'active': True},
    ), pytest.raises(
        ForbiddenError,
        match='Token audience does not include "audience"',
    ):
        authenticator.authenticate_user({'Authorization': 'Bearer <TOKEN>'})


def test_authenticate_user_with_token_identity_set() -> None:
    auth_client = globus_sdk.ConfidentialAppAuthClient(str(uuid.uuid4()), '')
    authenticator = GlobusAuthenticator(auth_client)
    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value={'active': True, 'aud': [authenticator._audience]},
    ), pytest.raises(
        ForbiddenError,
        match='The identity set of the token does not match this application',
    ):
        authenticator.authenticate_user({'Authorization': 'Bearer <TOKEN>'})


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

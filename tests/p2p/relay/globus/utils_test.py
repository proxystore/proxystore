from __future__ import annotations

import uuid
from typing import Any
from unittest import mock

import globus_sdk
import pytest
import websockets

from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError
from proxystore.p2p.relay.globus.utils import authenticate_user_with_token
from proxystore.p2p.relay.globus.utils import get_token_from_header
from proxystore.p2p.relay.globus.utils import GlobusUser


@pytest.fixture()
def auth_client() -> globus_sdk.ConfidentialAppAuthClient:
    return globus_sdk.ConfidentialAppAuthClient(
        client_id=uuid.uuid4(),
        client_secret='secret',
    )


def test_globus_user_equality() -> None:
    user1 = GlobusUser('username', uuid.uuid4())
    user2 = GlobusUser('username', uuid.uuid4())
    assert user1 != user2

    user2 = GlobusUser('different-username', user1.client_id)
    assert user1 == user2

    assert user1 != object()


def test_authenticate_user_with_token(
    auth_client: globus_sdk.ConfidentialAppAuthClient,
) -> None:
    token_meta: dict[str, Any] = {
        'active': True,
        'aud': ['audience'],
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
        user = authenticate_user_with_token(auth_client, 'token', 'audience')

    assert user == GlobusUser(
        username=token_meta['username'],
        client_id=uuid.UUID(token_meta['client_id']),
        email=token_meta['email'],
        display_name=token_meta['name'],
    )


def test_authenticate_user_with_token_expired_token(
    auth_client: globus_sdk.ConfidentialAppAuthClient,
) -> None:
    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value={'active': False},
    ), pytest.raises(
        ForbiddenError,
        match='Token is expired or has been revoked.',
    ):
        authenticate_user_with_token(auth_client, 'token')


def test_authenticate_user_with_token_wrong_audience(
    auth_client: globus_sdk.ConfidentialAppAuthClient,
) -> None:
    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value={'active': True},
    ), pytest.raises(
        ForbiddenError,
        match='Token audience does not include "audience"',
    ):
        authenticate_user_with_token(auth_client, 'token', 'audience')


def test_authenticate_user_with_token_identity_set(
    auth_client: globus_sdk.ConfidentialAppAuthClient,
) -> None:
    with mock.patch.object(
        auth_client,
        'oauth2_token_introspect',
        return_value={'active': True, 'aud': ['audience']},
    ), pytest.raises(
        ForbiddenError,
        match='The identity set of the token does not match this application',
    ):
        authenticate_user_with_token(auth_client, 'token', 'audience')


def test_get_token_from_header() -> None:
    options = {'Authorization': 'Bearer <TOKEN>'}
    headers = websockets.datastructures.Headers(**options)
    assert get_token_from_header(headers) == '<TOKEN>'


def test_get_token_from_header_missing() -> None:
    with pytest.raises(
        UnauthorizedError,
        match='Opening handshake from client is missing bearer token',
    ):
        get_token_from_header(websockets.datastructures.Headers())


def test_get_token_from_header_malformed() -> None:
    options = {'Authorization': '<TOKEN>'}
    headers = websockets.datastructures.Headers(**options)
    with pytest.raises(
        UnauthorizedError,
        match='Bearer token in authorization header is malformed.',
    ):
        get_token_from_header(headers)

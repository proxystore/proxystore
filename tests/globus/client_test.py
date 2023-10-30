from __future__ import annotations

import os
import uuid
from unittest import mock

import globus_sdk
import pytest

from proxystore.globus.client import get_confidential_app_auth_client
from proxystore.globus.client import get_native_app_auth_client
from proxystore.globus.client import is_client_login
from proxystore.globus.client import PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME
from proxystore.globus.client import PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME


def test_get_confidential_app_auth_client() -> None:
    client_uuid = str(uuid.uuid4())
    client_secret = 'secret'
    client = get_confidential_app_auth_client(client_uuid, client_secret)
    assert isinstance(client, globus_sdk.ConfidentialAppAuthClient)
    assert client.client_id == client_uuid


def test_get_confidential_app_auth_client_from_env() -> None:
    client_uuid = str(uuid.uuid4())
    client_secret = 'secret'

    env = {
        PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME: client_uuid,
        PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME: client_secret,
    }
    with mock.patch.dict(os.environ, env):
        client = get_confidential_app_auth_client()

    assert isinstance(client, globus_sdk.ConfidentialAppAuthClient)
    assert client.client_id == client_uuid


def test_get_confidential_app_auth_client_from_env_missing() -> None:
    with mock.patch.dict(os.environ, {}):
        with pytest.raises(ValueError, match='Either set both environment'):
            get_confidential_app_auth_client()


def test_get_confidential_app_auth_client_bad_uuid() -> None:
    with pytest.raises(
        ValueError,
        match='Client ID "abc" is not a valid UUID.',
    ):
        get_confidential_app_auth_client('abc', 'secret')


def test_get_native_app_auth_client() -> None:
    client = get_native_app_auth_client()
    assert isinstance(client, globus_sdk.NativeAppAuthClient)


def test_is_client_login() -> None:
    with mock.patch.dict(os.environ, {}):
        assert not is_client_login()

    env = {PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME: str(uuid.uuid4())}
    with mock.patch.dict(os.environ, env):
        assert not is_client_login()

    env[PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME] = 'secret'
    with mock.patch.dict(os.environ, env):
        assert is_client_login()

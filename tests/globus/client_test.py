from __future__ import annotations

import os
import uuid
from unittest import mock

import globus_sdk
import pytest

from proxystore.globus.app import PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME
from proxystore.globus.app import PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME
from proxystore.globus.client import get_confidential_app_auth_client
from proxystore.globus.client import get_native_app_auth_client
from proxystore.globus.client import get_transfer_client
from testing.mocked.globus import get_testing_app


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


def test_get_native_app_auth_client() -> None:
    client = get_native_app_auth_client()
    assert isinstance(client, globus_sdk.NativeAppAuthClient)


def test_get_transfer_client_default() -> None:
    with mock.patch(
        'proxystore.globus.client.get_globus_app',
        return_value=get_testing_app(),
    ):
        client = get_transfer_client()
    assert isinstance(client, globus_sdk.TransferClient)


def test_get_transfer_client_custom() -> None:
    globus_app = get_testing_app()
    # App.add_scope_requirements() is called by
    # TransferClient.add_app_data_access_scope() which calls
    # TransferClient.add_app_scope().
    with mock.patch.object(
        globus_app,
        'add_scope_requirements',
    ) as mock_add_scope:
        with mock.patch(
            'proxystore.globus.client.uses_data_access',
            side_effect=(True, False),
        ) as mock_data_access:
            client = get_transfer_client(
                globus_app,
                collections=[str(uuid.uuid4()), str(uuid.uuid4())],
            )
    assert mock_add_scope.call_count == 2
    assert mock_data_access.call_count == 2
    assert isinstance(client, globus_sdk.TransferClient)

from __future__ import annotations

import os
import pathlib
import uuid
from unittest import mock

import globus_sdk
import pytest
from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import TransferScopes
from globus_sdk.tokenstorage import SQLiteAdapter

from proxystore.globus.client import PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME
from proxystore.globus.client import PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME
from proxystore.globus.manager import ConfidentialAppAuthManager
from proxystore.globus.manager import NativeAppAuthManager

CLIENT_IDENTITY_ENV = {
    PROXYSTORE_GLOBUS_CLIENT_ID_ENV_NAME: str(uuid.uuid4()),
    PROXYSTORE_GLOBUS_CLIENT_SECRET_ENV_NAME: '<SECRET>',
}


@pytest.fixture()
def storage(tmp_path: pathlib.Path) -> SQLiteAdapter:
    return SQLiteAdapter(':memory:')


@mock.patch.dict(os.environ, CLIENT_IDENTITY_ENV)
def test_confidential_app_auth_manager_login(
    storage: SQLiteAdapter,
) -> None:
    manager = ConfidentialAppAuthManager(storage=storage)
    assert isinstance(manager.client, globus_sdk.ConfidentialAppAuthClient)
    assert manager.logged_in

    # Login is a no-op so will not change the state of the manager
    manager.login()
    assert manager.logged_in


@mock.patch.dict(os.environ, CLIENT_IDENTITY_ENV)
@mock.patch('globus_sdk.ClientCredentialsAuthorizer')
def test_confidential_app_auth_manager_get_authorizer(
    mock_authorizer,
    storage: SQLiteAdapter,
) -> None:
    manager = ConfidentialAppAuthManager(storage=storage)

    manager.get_authorizer(AuthScopes.resource_server)

    tokens = {'access_token': '<TOKEN>', 'expires_at_seconds': 0}
    with mock.patch.object(
        manager._storage,
        'get_token_data',
        return_value=tokens,
    ):
        manager.get_authorizer(AuthScopes.resource_server)


@mock.patch.dict(os.environ, CLIENT_IDENTITY_ENV)
@mock.patch('globus_sdk.ConfidentialAppAuthClient.oauth2_revoke_token')
def test_confidential_app_auth_manager_logout(
    mock_revoke,
    storage: SQLiteAdapter,
) -> None:
    data = {
        'auth.api.globus.org': {
            'access_token': 'xxx',
            'refresh_token': 'xxx',
        },
        'transfer.api.globus.org': {
            'access_token': 'xxx',
            'refresh_token': 'xxx',
        },
    }

    manager = ConfidentialAppAuthManager(storage=storage)

    with mock.patch.object(
        manager._storage,
        'get_by_resource_server',
        return_value=data,
    ), mock.patch.object(
        manager._storage,
        'remove_tokens_for_resource_server',
    ) as mock_remove_tokens:
        manager.logout()

        assert mock_remove_tokens.call_count == len(data)

    assert mock_revoke.call_count == 2 * len(data)


def test_native_app_auth_manager_not_authenticated(
    storage: SQLiteAdapter,
) -> None:
    manager = NativeAppAuthManager(storage=storage)
    assert isinstance(manager.client, globus_sdk.NativeAppAuthClient)
    assert not manager.logged_in
    with pytest.raises(
        LookupError,
        match=f'Could not find tokens for {AuthScopes.resource_server}.',
    ):
        manager.get_authorizer(AuthScopes.resource_server)

    # No tokens so nothing should happen
    manager.logout()


def test_native_app_auth_manager_logged_in_property(
    storage: SQLiteAdapter,
) -> None:
    manager = NativeAppAuthManager(
        storage=storage,
        resource_server_scopes={AuthScopes.resource_server: []},
    )

    assert not manager.logged_in

    with mock.patch.object(
        manager._storage,
        'get_by_resource_server',
        return_value={
            AuthScopes.resource_server: [],
            TransferScopes.resource_server: [],
        },
    ):
        assert manager.logged_in


@mock.patch('globus_sdk.tokenstorage.SQLiteAdapter.store')
@mock.patch(
    'proxystore.globus.manager.NativeAppAuthManager.logged_in',
    new_callable=mock.PropertyMock,
)
@mock.patch('click.prompt', return_value='fake-auth-code')
@mock.patch('globus_sdk.NativeAppAuthClient.oauth2_exchange_code_for_tokens')
@mock.patch('globus_sdk.NativeAppAuthClient.oauth2_get_authorize_url')
@mock.patch('globus_sdk.NativeAppAuthClient.oauth2_start_flow')
def test_native_app_auth_manager_login_flow(
    mock_oauth2_start_flow,
    mock_oauth2_get_authorize_url,
    mock_oauth2_exchange_code_for_tokens,
    mock_prompt,
    mock_logged_in,
    mock_store,
    storage: SQLiteAdapter,
) -> None:
    mock_logged_in.return_value = False

    manager = NativeAppAuthManager(storage=storage)

    # Mock these click methods so they don't print to stdout
    with mock.patch('click.echo'), mock.patch('click.secho'):
        manager.login()

    mock_store.assert_called_once()
    mock_prompt.assert_called_once()
    mock_oauth2_start_flow.assert_called_once()
    mock_oauth2_get_authorize_url.assert_called_once()
    mock_oauth2_exchange_code_for_tokens.assert_called_once()

    # Test idempotency
    mock_logged_in.return_value = True
    manager.login()

    # None of these should have been called again
    mock_store.assert_called_once()
    mock_prompt.assert_called_once()
    mock_oauth2_start_flow.assert_called_once()
    mock_oauth2_get_authorize_url.assert_called_once()
    mock_oauth2_exchange_code_for_tokens.assert_called_once()


@mock.patch('globus_sdk.RefreshTokenAuthorizer')
def test_native_app_auth_manager_get_authorizer(
    mock_authorizer,
    storage: SQLiteAdapter,
) -> None:
    manager = NativeAppAuthManager(storage=storage)

    with mock.patch.object(manager._storage, 'get_token_data'):
        manager.get_authorizer(AuthScopes.resource_server)


@mock.patch('globus_sdk.NativeAppAuthClient.oauth2_revoke_token')
def test_native_app_auth_manager_logout(
    mock_revoke,
    storage: SQLiteAdapter,
) -> None:
    data = {
        'auth.api.globus.org': {
            'access_token': 'xxx',
            'refresh_token': 'xxx',
        },
        'transfer.api.globus.org': {
            'access_token': 'xxx',
            'refresh_token': 'xxx',
        },
    }

    manager = NativeAppAuthManager(storage=storage)

    with mock.patch.object(
        manager._storage,
        'get_by_resource_server',
        return_value=data,
    ), mock.patch.object(
        manager._storage,
        'remove_tokens_for_resource_server',
    ) as mock_remove_tokens:
        manager.logout()

        assert mock_remove_tokens.call_count == len(data)

    assert mock_revoke.call_count == 2 * len(data)

from __future__ import annotations

from unittest import mock

import globus_sdk
import pytest
from globus_sdk.scopes import AuthScopes
from globus_sdk.scopes import TransferScopes

from proxystore.globus.manager import NativeAppAuthManager


def test_native_app_auth_manager_not_authenticated() -> None:
    manager = NativeAppAuthManager()
    assert isinstance(manager.client, globus_sdk.NativeAppAuthClient)
    assert not manager.logged_in
    with pytest.raises(
        LookupError,
        match=f'Could not find tokens for {AuthScopes.resource_server}.',
    ):
        manager.get_authorizer(AuthScopes.resource_server)

    # No tokens so nothing should happen
    manager.logout()


def test_native_app_auth_manager_logged_in_property() -> None:
    manager = NativeAppAuthManager(
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
) -> None:
    mock_logged_in.return_value = False

    manager = NativeAppAuthManager()

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
def test_native_app_auth_manager_get_authorizer(mock_authorizer) -> None:
    manager = NativeAppAuthManager()

    with mock.patch.object(manager._storage, 'get_token_data'):
        manager.get_authorizer(AuthScopes.resource_server)


@mock.patch('globus_sdk.NativeAppAuthClient.oauth2_revoke_token')
def test_native_app_auth_manager_logout(mock_revoke) -> None:
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

    manager = NativeAppAuthManager()

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

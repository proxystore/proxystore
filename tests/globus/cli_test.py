from __future__ import annotations

import uuid
from unittest import mock

import click
import click.testing
import pytest
from globus_sdk.tokenstorage import TokenStorageData

from proxystore.globus.cli import cli
from proxystore.globus.cli import login
from proxystore.globus.cli import logout
from testing.mocked.globus import get_testing_app


def test_cli() -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 0


@mock.patch('proxystore.globus.cli.get_user_app')
@mock.patch('proxystore.globus.cli.get_all_scopes_by_resource_server')
def test_cli_login_flow(mock_get_scopes, mock_get_user_app) -> None:
    globus_app = get_testing_app()
    mock_get_user_app.return_value = globus_app
    mock_get_scopes.return_value = {'server': ['scope']}

    runner = click.testing.CliRunner()

    with (
        mock.patch.object(
            globus_app,
            'login',
            return_value=None,
        ) as mock_run_login_flow,
        mock.patch.object(
            globus_app._token_storage,
            'get_token_data_by_resource_server',
            return_value={},
        ),
    ):
        result = runner.invoke(login)

    assert result.exit_code == 0
    mock_get_user_app.assert_called_once()
    mock_get_scopes.assert_called_once()
    mock_run_login_flow.assert_called_once()


@mock.patch('proxystore.globus.cli.get_user_app')
@mock.patch('proxystore.globus.cli.get_all_scopes_by_resource_server')
def test_cli_login_flow_with_args(mock_get_scopes, mock_get_user_app) -> None:
    globus_app = get_testing_app()
    mock_get_user_app.return_value = globus_app
    mock_get_scopes.return_value = {'server': ['scope']}

    runner = click.testing.CliRunner()

    with (
        mock.patch.object(
            globus_app,
            'login',
            return_value=None,
        ) as mock_run_login_flow,
        mock.patch.object(
            globus_app._token_storage,
            'get_token_data_by_resource_server',
            return_value={},
        ),
    ):
        result = runner.invoke(
            login,
            ['-c', str(uuid.uuid4()), '-d', 'globus.org'],
        )

    assert result.exit_code == 0
    mock_get_user_app.assert_called_once()
    mock_get_scopes.assert_called_once()
    mock_run_login_flow.assert_called_once()


@mock.patch('proxystore.globus.cli.get_user_app')
def test_cli_already_logged_in(mock_get_user_app) -> None:
    globus_app = get_testing_app()
    mock_get_user_app.return_value = globus_app

    runner = click.testing.CliRunner()

    with (
        mock.patch.object(
            globus_app,
            '_run_login_flow',
            return_value=None,
        ) as mock_run_login_flow,
        mock.patch.object(
            globus_app,
            'login_required',
            return_value=False,
        ) as mock_login_required,
    ):
        result = runner.invoke(login)

    assert result.exit_code == 0
    mock_get_user_app.assert_called_once()
    mock_login_required.assert_called_once()
    mock_run_login_flow.assert_not_called()


@pytest.mark.parametrize('refresh_token', (None, 'mock_refresh_token'))
@mock.patch('proxystore.globus.cli.get_user_app')
@mock.patch('globus_sdk.NativeAppAuthClient.oauth2_revoke_token')
def test_cli_logout(
    mock_native_client,
    mock_get_user_app,
    refresh_token: str | None,
) -> None:
    globus_app = get_testing_app()
    mock_get_user_app.return_value = globus_app

    token_data = TokenStorageData(
        resource_server='auth.globus.org',
        identity_id='mock_identity_id',
        scope='openid',
        access_token='mock_access_token',
        refresh_token=refresh_token,
        expires_at_seconds=0,
        token_type='Bearer',
    )
    scopes = {token_data.resource_server: token_data}

    runner = click.testing.CliRunner()

    with mock.patch.object(
        globus_app._token_storage,
        'get_token_data_by_resource_server',
        return_value=scopes,
    ):
        result = runner.invoke(logout)

    assert result.exit_code == 0
    mock_get_user_app.assert_called_once()

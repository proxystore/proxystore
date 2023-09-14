from __future__ import annotations

from unittest import mock

import click
import click.testing

from proxystore.globus.cli import cli
from proxystore.globus.cli import login
from proxystore.globus.cli import logout


def test_cli() -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 0


@mock.patch('proxystore.globus.cli.NativeAppAuthManager.login')
@mock.patch(
    'proxystore.globus.cli.NativeAppAuthManager.logged_in',
    new_callable=mock.PropertyMock,
)
def test_cli_login(mock_logged_in, mock_login) -> None:
    mock_logged_in.return_value = False
    runner = click.testing.CliRunner()
    result = runner.invoke(login)
    assert result.exit_code == 0
    mock_login.assert_called_once()


@mock.patch('proxystore.globus.cli.NativeAppAuthManager.login')
@mock.patch(
    'proxystore.globus.cli.NativeAppAuthManager.logged_in',
    new_callable=mock.PropertyMock,
)
def test_cli_already_logged_in(mock_logged_in, mock_login) -> None:
    mock_logged_in.return_value = True
    runner = click.testing.CliRunner()
    result = runner.invoke(login)
    assert result.exit_code == 0
    mock_login.assert_not_called()


@mock.patch('proxystore.globus.cli.NativeAppAuthManager.logout')
def test_cli_logout(mock_logout) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(logout)
    assert result.exit_code == 0
    mock_logout.assert_called_once()

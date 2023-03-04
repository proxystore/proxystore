from __future__ import annotations

import logging
import os
import pathlib
from typing import Generator
from unittest import mock

import click
import pytest

import proxystore
from proxystore.endpoint.cli import cli
from proxystore.endpoint.config import read_config


@pytest.fixture()
def home_dir(tmp_path: pathlib.Path) -> Generator[str, None, None]:
    with mock.patch(
        'proxystore.utils.home_dir',
        return_value=str(tmp_path),
    ), mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        yield str(tmp_path)


def test_no_command() -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 0
    assert result.output.startswith('Usage:')


def test_help_command() -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['help'])
    assert result.exit_code == 0
    assert result.output.startswith('Usage:')


def test_version_command() -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['version'])
    assert result.exit_code == 0
    assert result.output.strip() == f'ProxyStore v{proxystore.__version__}'


def test_configure_command(home_dir) -> None:
    name = 'my-endpoint'
    port = 4321
    relay_server = 'ws://server:1234'
    args = [name, '--port', str(port), '--relay-server', relay_server]

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['configure', *args])
    assert result.exit_code == 0

    endpoint_dir = os.path.join(home_dir, name)
    assert os.path.isdir(endpoint_dir)
    cfg = read_config(endpoint_dir)
    assert cfg.name == name
    assert cfg.port == port
    assert cfg.relay_server == relay_server


def test_list_command(home_dir, caplog) -> None:
    # Note: because home_dir is mocked, there's nothing to list so we
    # are really testing that the correct command in
    # proxystore.endpoint.commands is called and leaving the testing of that
    # command to tests/endpoint/commands_test.py.
    caplog.set_level(logging.INFO)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['list'])
    assert result.exit_code == 0
    assert len(caplog.records) == 1
    assert 'No valid endpoint configurations' in caplog.records[0].message


def test_remove_command(home_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['remove', 'myendpoint'])
    assert result.exit_code == 1
    assert len(caplog.records) == 1
    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_start_command(home_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['start', 'myendpoint'])
    assert result.exit_code == 1
    assert len(caplog.records) == 2
    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_stop_command(home_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['stop', 'myendpoint'])
    assert result.exit_code == 1
    assert len(caplog.records) == 2
    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )

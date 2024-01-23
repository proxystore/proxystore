from __future__ import annotations

import logging
import os
import pathlib
import uuid
from typing import Generator
from unittest import mock

import click
import pytest
import requests

import proxystore
from proxystore.endpoint.cli import cli
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config
from proxystore.p2p.nat import NatType
from proxystore.p2p.nat import Result


@pytest.fixture()
def home_dir(tmp_path: pathlib.Path) -> Generator[str, None, None]:
    with mock.patch(
        'proxystore.utils.environment.home_dir',
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


def test_check_nat_normal(caplog) -> None:
    caplog.set_level(logging.INFO)
    runner = click.testing.CliRunner()

    r = Result(NatType.RestrictedCone, '192.168.1.1', 1234)
    with mock.patch('proxystore.p2p.nat.check_nat', return_value=r):
        result = runner.invoke(cli, ['check-nat'])

    assert result.exit_code == 0
    assert caplog.records[1].message == 'NAT Type:       Restricted-cone NAT'
    assert caplog.records[2].message == 'External IP:    192.168.1.1'
    assert caplog.records[3].message == 'External Port:  1234'
    assert caplog.records[4].message.startswith(
        'NAT traversal for peer-to-peer methods (e.g., hole-punching) '
        'is likely to work.',
    )


def test_configure_command(home_dir) -> None:
    name = 'my-endpoint'
    port = 4321
    relay_server = 'ws://server:1234'
    args = [name, '--port', str(port), '--relay-address', relay_server]

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['configure', *args])
    assert result.exit_code == 0

    endpoint_dir = os.path.join(home_dir, name)
    assert os.path.isdir(endpoint_dir)
    cfg = read_config(endpoint_dir)
    assert cfg.name == name
    assert cfg.port == port
    assert cfg.relay.address == relay_server


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


def test_test_command_missing_endpoint(home_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch('proxystore.endpoint.cli.home_dir', return_value=home_dir):
        runner = click.testing.CliRunner()

        result = runner.invoke(cli, ['test', 'fake-name', 'exists', 'key'])
        assert result.exit_code == 1
        assert (
            'An endpoint named fake-name does not exist.'
            in caplog.records[0].message
        )


def test_test_command(home_dir, caplog, endpoint: EndpointConfig) -> None:
    caplog.set_level(logging.INFO)

    with mock.patch('proxystore.endpoint.cli.home_dir', return_value=home_dir):
        endpoint_dir = os.path.join(home_dir, endpoint.name)
        write_config(endpoint, endpoint_dir)

        runner = click.testing.CliRunner()

        value = 'hello hello'
        key_uuid = uuid.uuid4()
        key = str(key_uuid)

        with mock.patch('uuid.uuid4', return_value=key_uuid):
            result = runner.invoke(cli, ['test', endpoint.name, 'put', value])
        assert result.exit_code == 0
        assert key in caplog.records[0].message
        caplog.clear()

        result = runner.invoke(cli, ['test', endpoint.name, 'exists', key])
        assert result.exit_code == 0
        assert 'True' in caplog.records[0].message
        caplog.clear()

        result = runner.invoke(cli, ['test', endpoint.name, 'get', key])
        assert result.exit_code == 0
        assert value in caplog.records[0].message
        caplog.clear()

        result = runner.invoke(cli, ['test', endpoint.name, 'evict', key])
        assert result.exit_code == 0
        caplog.clear()

        result = runner.invoke(cli, ['test', endpoint.name, 'exists', key])
        assert result.exit_code == 0
        assert 'False' in caplog.records[0].message
        caplog.clear()

        result = runner.invoke(cli, ['test', endpoint.name, 'get', key])
        assert result.exit_code == 0
        assert 'does not exist' in caplog.records[0].message
        caplog.clear()


def test_test_command_connection_error(
    home_dir,
    caplog,
    endpoint: EndpointConfig,
) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch('proxystore.endpoint.cli.home_dir', return_value=home_dir):
        endpoint_dir = os.path.join(home_dir, endpoint.name)
        write_config(endpoint, endpoint_dir)

        runner = click.testing.CliRunner()
        key = 'fake-key'

        with mock.patch(
            'proxystore.endpoint.client.evict',
            side_effect=requests.exceptions.ConnectionError,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'evict', key])
        assert result.exit_code == 1
        assert 'Unable to connect' in caplog.records[0].message
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.exists',
            side_effect=requests.exceptions.ConnectionError,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'exists', key])
        assert 'Unable to connect' in caplog.records[0].message
        assert result.exit_code == 1
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.get',
            side_effect=requests.exceptions.ConnectionError,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'get', key])
        assert 'Unable to connect' in caplog.records[0].message
        assert result.exit_code == 1
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.put',
            side_effect=requests.exceptions.ConnectionError,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'put', key])
        assert 'Unable to connect' in caplog.records[0].message
        assert result.exit_code == 1
        caplog.clear()


def test_test_command_unexpected_error(
    home_dir,
    caplog,
    endpoint: EndpointConfig,
) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch('proxystore.endpoint.cli.home_dir', return_value=home_dir):
        endpoint_dir = os.path.join(home_dir, endpoint.name)
        write_config(endpoint, endpoint_dir)

        runner = click.testing.CliRunner()
        key = 'fake-key'

        with mock.patch(
            'proxystore.endpoint.client.evict',
            side_effect=requests.exceptions.RequestException,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'evict', key])
        assert result.exit_code == 1
        assert len(caplog.records) == 1
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.exists',
            side_effect=requests.exceptions.RequestException,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'exists', key])
        assert result.exit_code == 1
        assert len(caplog.records) == 1
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.get',
            side_effect=requests.exceptions.RequestException,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'get', key])
        assert result.exit_code == 1
        assert len(caplog.records) == 1
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.put',
            side_effect=requests.exceptions.RequestException,
        ):
            result = runner.invoke(cli, ['test', endpoint.name, 'put', key])
        assert result.exit_code == 1
        assert len(caplog.records) == 1
        caplog.clear()

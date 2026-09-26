from __future__ import annotations

import asyncio
import importlib.metadata
import logging
import os
import pathlib
import uuid
from collections.abc import Generator
from unittest import mock

import click
import click.testing
import pytest

import proxystore
from proxystore.endpoint.cli import cli
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointFiles
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.serve import _serve_async
from proxystore.p2p.nat import NatMapping
from proxystore.p2p.nat import Result
from testing.endpoint import copy_endpoint_dir
from testing.endpoint import wait_for_endpoint
from testing.utils import open_port

CLICK_VERSION = tuple(
    int(x) for x in importlib.metadata.version('click').split('.')
)


@pytest.fixture
def home_dir(tmp_path: pathlib.Path) -> Generator[str, None, None]:
    with (
        mock.patch(
            'proxystore.utils.environment.home_dir',
            return_value=str(tmp_path),
        ),
        mock.patch(
            'proxystore.endpoint.commands.home_dir',
            return_value=str(tmp_path),
        ),
    ):
        yield str(tmp_path)


def test_no_command() -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli)
    # https://github.com/pallets/click/pull/1489
    expected = 2 if CLICK_VERSION >= (8, 2, 0) else 0
    assert result.exit_code == expected
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

    r = Result(
        NatMapping.EndpointIndependent,
        '192.168.1.1',
        1234,
        True,
    )
    with mock.patch(
        'proxystore.p2p.nat.check_nat',
        mock.AsyncMock(return_value=r),
    ):
        result = runner.invoke(cli, ['check-nat'])

    assert result.exit_code == 0
    assert caplog.records[1].message == (
        'NAT Behavior:   Endpoint-independent mapping'
    )
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
    assert not cfg.tls

    result = runner.invoke(cli, ['configure', 'tls-endpoint', '--tls'])
    assert result.exit_code == 0
    assert read_config(os.path.join(home_dir, 'tls-endpoint')).tls


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


def test_test_command(
    home_dir,
    caplog,
    endpoint: EndpointConfig,
    endpoint_dir: str,
) -> None:
    caplog.set_level(logging.INFO)

    with mock.patch('proxystore.endpoint.cli.home_dir', return_value=home_dir):
        copy_endpoint_dir(endpoint_dir, home_dir)

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


@pytest.mark.parametrize('command', ('evict', 'exists', 'get', 'put'))
def test_test_command_errors(
    command: str,
    home_dir,
    caplog,
    endpoint: EndpointConfig,
    endpoint_dir: str,
) -> None:
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    args = ['test', endpoint.name, command, 'fake-key']

    with mock.patch('proxystore.endpoint.cli.home_dir', return_value=home_dir):
        copied_dir = copy_endpoint_dir(endpoint_dir, home_dir)

        with mock.patch(
            'proxystore.endpoint.client.EndpointClient.connect',
            side_effect=ConnectionRefusedError,
        ):
            result = runner.invoke(cli, args)
        assert result.exit_code == 1
        assert 'Unable to connect' in caplog.records[0].message
        caplog.clear()

        with mock.patch(
            'proxystore.endpoint.client.EndpointClient.connect',
            side_effect=EndpointAuthError('auth failed'),
        ):
            result = runner.invoke(cli, args)
        assert result.exit_code == 1
        assert 'auth failed' in caplog.records[0].message
        caplog.clear()

        result = runner.invoke(
            cli,
            ['test', '--remote', 'not-a-uuid', endpoint.name, command, 'key'],
        )
        assert result.exit_code == 1
        assert 'not a valid UUID4' in caplog.records[0].message
        caplog.clear()

        os.remove(EndpointFiles(copied_dir).token)
        result = runner.invoke(cli, args)
        assert result.exit_code == 1
        assert 'Is the endpoint running?' in caplog.records[0].message
        caplog.clear()

        config = read_config(copied_dir)
        config.host = None
        write_config(config, copied_dir)
        result = runner.invoke(cli, args)
        assert result.exit_code == 1
        assert 'has not been started' in caplog.records[0].message


async def test_test_command_tls(home_dir, caplog) -> None:
    caplog.set_level(logging.INFO)
    config = EndpointConfig(
        name='tls-endpoint',
        uuid=str(uuid.uuid4()),
        host='127.0.0.1',
        port=open_port(),
        tls=True,
    )
    endpoint_dir = os.path.join(home_dir, config.name)
    write_config(config, endpoint_dir)

    stop = asyncio.Event()
    task = asyncio.create_task(_serve_async(config, endpoint_dir, stop))
    await asyncio.to_thread(wait_for_endpoint, '127.0.0.1', config.port)

    runner = click.testing.CliRunner()
    try:
        with mock.patch(
            'proxystore.endpoint.cli.home_dir',
            return_value=home_dir,
        ):
            result = await asyncio.to_thread(
                runner.invoke,
                cli,
                ['test', config.name, 'exists', 'key'],
            )
        assert result.exit_code == 0
        assert any('Object exists: False' in r.message for r in caplog.records)
    finally:
        stop.set()
        await task

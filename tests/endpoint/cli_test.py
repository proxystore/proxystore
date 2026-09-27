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
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.serve import running_endpoint
from testing.endpoint import copy_endpoint_dir
from testing.endpoint import write_endpoint

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
            'proxystore.endpoint.directory.home_dir',
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


def test_configure_command(home_dir) -> None:
    name = 'my-endpoint'
    port = 4321
    args = [name, '--port', str(port)]

    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['configure', *args])
    assert result.exit_code == 0

    endpoint_dir = EndpointDir(os.path.join(home_dir, name))
    assert os.path.isdir(endpoint_dir)
    cfg = endpoint_dir.read_config()
    assert cfg.name == name
    assert cfg.port == port
    assert not cfg.tls

    assert cfg.p2p.enabled

    assert cfg.p2p.relays == 'n0'

    urls = 'https://a.example.com, https://b.example.com'
    result = runner.invoke(cli, ['configure', 'relays', '--relays', urls])
    assert result.exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'relays'))
    assert endpoint_dir.read_config().p2p.relays == [
        'https://a.example.com',
        'https://b.example.com',
    ]

    result = runner.invoke(cli, ['configure', 'norelay', '--relays', 'none'])
    assert result.exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'norelay'))
    assert endpoint_dir.read_config().p2p.relays == 'none'

    result = runner.invoke(cli, ['configure', 'solo', '--no-peering'])
    assert result.exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'solo'))
    assert not endpoint_dir.read_config().p2p.enabled

    result = runner.invoke(cli, ['configure', 'tls-endpoint', '--tls'])
    assert result.exit_code == 0
    assert (
        EndpointDir(os.path.join(home_dir, 'tls-endpoint')).read_config().tls
    )


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
    assert any('does not exist' in record.message for record in caplog.records)


def test_start_command(home_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['start', 'myendpoint'])
    assert result.exit_code == 1
    assert len(caplog.records) == 2
    assert any('does not exist' in record.message for record in caplog.records)


def test_stop_command(home_dir, caplog) -> None:
    # Note: similar to test_list()
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['stop', 'myendpoint'])
    assert result.exit_code == 1
    assert len(caplog.records) == 2
    assert any('does not exist' in record.message for record in caplog.records)


def test_test_command_missing_endpoint(home_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['test', 'fake-name', 'exists', 'key'])
    assert result.exit_code == 1
    assert (
        'An endpoint named fake-name does not exist'
        in caplog.records[0].message
    )


def test_test_command(
    home_dir,
    caplog,
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    caplog.set_level(logging.INFO)
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
    endpoint_dir: EndpointDir,
) -> None:
    caplog.set_level(logging.ERROR)
    runner = click.testing.CliRunner()
    args = ['test', endpoint.name, command, 'fake-key']
    copied_dir = copy_endpoint_dir(endpoint_dir, home_dir)

    with mock.patch(
        'proxystore.endpoint.client.EndpointClient.connect',
        side_effect=EndpointNotRunningError('connection refused'),
    ):
        result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert 'connection refused' in caplog.records[0].message
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
    assert 'not a valid endpoint ID' in caplog.records[0].message
    caplog.clear()

    os.remove(copied_dir.connection_path)
    result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert 'Is the endpoint running?' in caplog.records[0].message


async def test_test_command_tls(home_dir, caplog) -> None:
    caplog.set_level(logging.INFO)
    endpoint_dir, config = write_endpoint(
        home_dir,
        'tls-endpoint',
        host='127.0.0.1',
        tls=True,
    )

    runner = click.testing.CliRunner()
    async with running_endpoint(endpoint_dir):
        result = await asyncio.to_thread(
            runner.invoke,
            cli,
            ['test', config.name, 'exists', 'key'],
        )
    assert result.exit_code == 0
    assert any('Object exists: False' in r.message for r in caplog.records)


def test_id_and_peers_commands(home_dir, caplog) -> None:
    caplog.set_level(logging.INFO)
    runner = click.testing.CliRunner()
    assert runner.invoke(cli, ['configure', 'ep']).exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'ep'))

    caplog.clear()
    assert runner.invoke(cli, ['id', 'ep']).exit_code == 0
    assert caplog.records[-1].message == endpoint_dir.read_config().id

    peer_id = EndpointId.random()
    result = runner.invoke(cli, ['peers', 'add', 'ep', 'peer', peer_id])
    assert result.exit_code == 0
    assert endpoint_dir.peers.read().peers == {'peer': peer_id}

    caplog.clear()
    assert runner.invoke(cli, ['peers', 'list', 'ep']).exit_code == 0
    assert caplog.records[-1].message.split() == ['peer', peer_id]

    result = runner.invoke(cli, ['peers', 'remove', 'ep', 'peer'])
    assert result.exit_code == 0
    assert endpoint_dir.peers.read().peers == {}


def test_ping_command_local(
    home_dir,
    caplog,
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    caplog.set_level(logging.INFO)
    copy_endpoint_dir(endpoint_dir, home_dir)
    runner = click.testing.CliRunner()
    args = ['test', endpoint.name, 'ping', '--count', '2', '--interval', '0']
    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    messages = [r.message for r in caplog.records]
    assert sum('Reply from local endpoint' in m for m in messages) == 2
    assert '2 ping(s): min/avg/max' in messages[-1]


def test_ping_command_remote(home_dir, caplog) -> None:
    caplog.set_level(logging.INFO)
    remote = EndpointId.random()
    results = [
        PingResult(200.0, True, 'https://relay.example.com', 30),
        PingResult(2.0, False, '1.2.3.4:5', 1),
    ]
    client = mock.MagicMock()
    client.ping.side_effect = results
    client.__enter__.return_value = client
    runner = click.testing.CliRunner()
    with mock.patch(
        'proxystore.endpoint.cli.EndpointClient.from_name',
        return_value=client,
    ):
        result = runner.invoke(
            cli,
            [
                'test',
                '--remote',
                remote,
                'ep',
                'ping',
                '--interval',
                '0',
                '--count',
                '2',
            ],
        )
    assert result.exit_code == 0
    messages = [r.message for r in caplog.records]
    assert messages[0] == (
        f'Reply from {remote}: time=200.00 ms '
        'path=relayed via https://relay.example.com (rtt 30 ms)'
    )
    assert messages[1] == (
        f'Reply from {remote}: time=2.00 ms '
        'path=direct to 1.2.3.4:5 (rtt 1 ms)'
    )
    assert messages[2] == '2 ping(s): min/avg/max = 2.00/101.00/200.00 ms'


def test_ping_command_error(home_dir, caplog) -> None:
    caplog.set_level(logging.ERROR)
    client = mock.MagicMock()
    client.ping.side_effect = EndpointRequestError('peer failed')
    client.__enter__.return_value = client
    runner = click.testing.CliRunner()
    with mock.patch(
        'proxystore.endpoint.cli.EndpointClient.from_name',
        return_value=client,
    ):
        result = runner.invoke(cli, ['test', 'ep', 'ping'])
    assert result.exit_code == 1
    assert 'peer failed' in caplog.records[-1].message

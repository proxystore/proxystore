from __future__ import annotations

import asyncio
import importlib.metadata
import os
import pathlib
import uuid
from unittest import mock

import click
import click.testing
import pytest

import proxystore
from proxystore.endpoint.cli import cli
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointNotRunningError
from proxystore.endpoint.exceptions import EndpointRequestError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import PingResult
from testing.endpoint import copy_endpoint_dir
from testing.endpoint import write_endpoint

CLICK_VERSION = tuple(
    int(x) for x in importlib.metadata.version('click').split('.')
)


@pytest.fixture
def home_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv('PROXYSTORE_HOME', str(tmp_path))
    return str(tmp_path)


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
    assert cfg.p2p.discovery == 'n0'
    assert cfg.storage.backend == 'memory'

    result = runner.invoke(cli, ['configure', 'nod', '--discovery', 'none'])
    assert result.exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'nod'))
    assert endpoint_dir.read_config().p2p.discovery == 'none'

    result = runner.invoke(cli, ['configure', 'persist', '--persist'])
    assert result.exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'persist'))
    assert endpoint_dir.read_config().storage.backend == 'sqlite'

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


def test_configure_command_errors(home_dir) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['configure', 'ep'])
    assert result.exit_code == 0
    assert 'Configured endpoint: ep' in result.output
    assert 'proxystore-endpoint start ep' in result.output

    result = runner.invoke(cli, ['configure', 'ep'])
    assert result.exit_code == 1
    assert 'already exists' in result.output
    assert 'remove and try again' in result.output

    result = runner.invoke(cli, ['configure', 'bad name'])
    assert result.exit_code == 1
    assert 'alphanumeric' in result.output


def test_list_command(home_dir) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['list'])
    assert result.exit_code == 0
    assert 'No valid endpoint configurations' in result.output

    ep1 = EndpointDir.create('ep1', home_dir)
    ep2 = EndpointDir.create('ep2', home_dir)
    result = runner.invoke(cli, ['list'])
    assert result.exit_code == 0
    rows = [line.split() for line in result.output.splitlines()]
    assert rows[0] == ['NAME', 'STATUS', 'ID']
    assert rows[2:] == [
        ['ep1', 'STOPPED', ep1.read_config().id],
        ['ep2', 'STOPPED', ep2.read_config().id],
    ]


def test_output_ignores_log_level(home_dir) -> None:
    # Results are printed regardless of the log level
    endpoint_dir = EndpointDir.create('ep', home_dir)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['--log-level', 'ERROR', 'id', 'ep'])
    assert result.exit_code == 0
    assert result.stdout.strip() == endpoint_dir.read_config().id


def test_remove_command(home_dir) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['remove', 'myendpoint'])
    assert result.exit_code == 1
    assert 'does not exist' in result.output
    assert 'proxystore-endpoint list' in result.output

    endpoint_dir = EndpointDir.create('myendpoint', home_dir)
    with mock.patch.object(
        EndpointDir,
        'status',
        return_value=EndpointStatus.RUNNING,
    ):
        result = runner.invoke(cli, ['remove', 'myendpoint'])
    assert result.exit_code == 1
    assert 'must be stopped' in result.output
    assert 'proxystore-endpoint stop' in result.output

    result = runner.invoke(cli, ['remove', 'myendpoint'])
    assert result.exit_code == 0
    assert 'Removed endpoint' in result.output
    assert not os.path.exists(endpoint_dir.path)


def test_start_command(home_dir) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['start', 'myendpoint'])
    assert result.exit_code == 1
    assert 'does not exist' in result.output

    endpoint_dir = EndpointDir.create('myendpoint', home_dir)
    with mock.patch(
        'proxystore.endpoint.cli.start_endpoint',
    ) as start_endpoint:
        result = runner.invoke(cli, ['start', 'myendpoint', '--no-detach'])
    assert result.exit_code == 0
    start_endpoint.assert_called_once_with(
        endpoint_dir,
        detach=False,
        log_level='INFO',
    )


def test_stop_command(home_dir) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['stop', 'myendpoint'])
    assert result.exit_code == 1
    assert 'does not exist' in result.output

    EndpointDir.create('myendpoint', home_dir)
    for stopped, message in (
        (True, 'has been stopped'),
        (False, 'not running'),
    ):
        with mock.patch(
            'proxystore.endpoint.cli.stop_endpoint',
            return_value=stopped,
        ):
            result = runner.invoke(cli, ['stop', 'myendpoint'])
        assert result.exit_code == 0
        assert message in result.output


def test_client_command_missing_endpoint(home_dir) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['client', 'fake-name', 'exists', 'key'])
    assert result.exit_code == 1
    assert 'An endpoint named fake-name does not exist' in result.output


def test_client_command(
    home_dir,
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    copy_endpoint_dir(endpoint_dir, home_dir)
    runner = click.testing.CliRunner()
    value = 'hello hello'
    key_uuid = uuid.uuid4()
    key = str(key_uuid)

    def _invoke(*args: str) -> str:
        result = runner.invoke(cli, ['client', endpoint.name, *args])
        assert result.exit_code == 0
        return result.output

    with mock.patch('uuid.uuid4', return_value=key_uuid):
        assert key in _invoke('put', value)
    assert 'Object exists: True' in _invoke('exists', key)
    assert value in _invoke('get', key)
    assert 'Evicted' in _invoke('evict', key)
    assert 'Object exists: False' in _invoke('exists', key)
    assert 'does not exist' in _invoke('get', key)


@pytest.mark.parametrize('command', ('evict', 'exists', 'get', 'put'))
def test_client_command_errors(
    command: str,
    home_dir,
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    runner = click.testing.CliRunner()
    args = ['client', endpoint.name, command, 'fake-key']
    copied_dir = copy_endpoint_dir(endpoint_dir, home_dir)

    with mock.patch(
        'proxystore.endpoint.client.EndpointClient.connect',
        side_effect=EndpointNotRunningError('connection refused'),
    ):
        result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert 'connection refused' in result.output

    with mock.patch(
        'proxystore.endpoint.client.EndpointClient.connect',
        side_effect=EndpointAuthError('auth failed'),
    ):
        result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert 'auth failed' in result.output

    result = runner.invoke(
        cli,
        ['client', '--target', 'not-a-uuid', endpoint.name, command, 'key'],
    )
    assert result.exit_code == 1
    assert 'not a valid endpoint ID' in result.output

    os.remove(copied_dir.connection_path)
    result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert 'Is the endpoint running?' in result.output


async def test_client_command_tls(home_dir) -> None:
    endpoint_dir, config = write_endpoint(
        home_dir,
        'tls-endpoint',
        host='127.0.0.1',
        tls=True,
    )

    runner = click.testing.CliRunner()
    async with Endpoint(endpoint_dir):
        result = await asyncio.to_thread(
            runner.invoke,
            cli,
            ['client', config.name, 'exists', 'key'],
        )
    assert result.exit_code == 0
    assert 'Object exists: False' in result.output


def test_id_and_peers_commands(home_dir) -> None:
    runner = click.testing.CliRunner()
    assert runner.invoke(cli, ['configure', 'ep']).exit_code == 0
    endpoint_dir = EndpointDir(os.path.join(home_dir, 'ep'))

    result = runner.invoke(cli, ['id', 'ep'])
    assert result.exit_code == 0
    assert result.output.strip() == endpoint_dir.read_config().id

    peer_id = EndpointId.random()
    result = runner.invoke(cli, ['peers', 'add', 'ep', 'peer', peer_id])
    assert result.exit_code == 0
    assert f'Added peer peer <{peer_id}>' in result.output
    assert endpoint_dir.peers().read().peers == {'peer': peer_id}

    result = runner.invoke(cli, ['peers', 'list', 'ep'])
    assert result.exit_code == 0
    assert result.output.splitlines()[-1].split() == ['peer', peer_id]

    result = runner.invoke(cli, ['peers', 'remove', 'ep', 'peer'])
    assert result.exit_code == 0
    assert 'Removed peer peer' in result.output
    assert endpoint_dir.peers().read().peers == {}


def test_ping_command_local(
    home_dir,
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    copy_endpoint_dir(endpoint_dir, home_dir)
    runner = click.testing.CliRunner()
    args = ['client', endpoint.name, 'ping', '--count', '2', '--interval', '0']
    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert sum('Reply from local endpoint' in line for line in lines) == 2
    assert '2 ping(s): min/avg/max' in lines[-1]


def test_ping_command_target(home_dir) -> None:
    target = EndpointId.random()
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
                'client',
                '--target',
                target,
                'ep',
                'ping',
                '--interval',
                '0',
                '--count',
                '2',
            ],
        )
    assert result.exit_code == 0
    assert result.output.splitlines() == [
        (
            f'Reply from {target}: time=200.00 ms '
            'path=relayed via https://relay.example.com (rtt 30 ms)'
        ),
        (
            f'Reply from {target}: time=2.00 ms '
            'path=direct to 1.2.3.4:5 (rtt 1 ms)'
        ),
        '2 ping(s): min/avg/max = 2.00/101.00/200.00 ms',
    ]


def test_ping_command_error(home_dir) -> None:
    client = mock.MagicMock()
    client.ping.side_effect = EndpointRequestError('peer failed')
    client.__enter__.return_value = client
    runner = click.testing.CliRunner()
    with mock.patch(
        'proxystore.endpoint.cli.EndpointClient.from_name',
        return_value=client,
    ):
        result = runner.invoke(cli, ['client', 'ep', 'ping'])
    assert result.exit_code == 1
    assert 'Error: peer failed' in result.output


def test_peers_command_errors(home_dir) -> None:
    runner = click.testing.CliRunner()
    peer_id = EndpointId.random()

    # Missing endpoint
    for args in (
        ['id', 'ep'],
        ['peers', 'add', 'ep', 'peer', peer_id],
        ['peers', 'remove', 'ep', 'peer'],
        ['peers', 'list', 'ep'],
    ):
        result = runner.invoke(cli, args)
        assert result.exit_code == 1
        assert 'does not exist' in result.output

    EndpointDir.create('ep', home_dir)
    args = ['peers', 'add', 'ep', 'peer', peer_id]
    assert runner.invoke(cli, args).exit_code == 0

    args = ['peers', 'add', 'ep', 'peer', EndpointId.random()]
    result = runner.invoke(cli, args)
    assert result.exit_code == 1
    assert 'already exists' in result.output
    assert 'peers remove ep peer' in result.output

    result = runner.invoke(cli, ['peers', 'remove', 'ep', 'x'])
    assert result.exit_code == 1
    assert 'No peer named x' in result.output


def test_peers_list_empty(home_dir) -> None:
    EndpointDir.create('ep', home_dir)
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['peers', 'list', 'ep'])
    assert result.exit_code == 0
    assert 'has no peers' in result.output


@pytest.mark.parametrize(
    ('host', 'expected'),
    (('IP', 'ip'), (' fqdn ', 'fqdn'), ('127.0.0.1', '127.0.0.1')),
)
def test_configure_command_host(home_dir, host: str, expected: str) -> None:
    runner = click.testing.CliRunner()
    result = runner.invoke(cli, ['configure', 'ep', '--host', host])
    assert result.exit_code == 0
    assert EndpointDir.from_name('ep', home_dir).read_config().host == expected

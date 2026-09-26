from __future__ import annotations

import logging
import multiprocessing
import os
import pathlib
import time
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.commands import _wait_for_exit
from proxystore.endpoint.commands import add_peer
from proxystore.endpoint.commands import configure_endpoint
from proxystore.endpoint.commands import EndpointStatus
from proxystore.endpoint.commands import get_endpoint_id
from proxystore.endpoint.commands import get_status
from proxystore.endpoint.commands import list_endpoints
from proxystore.endpoint.commands import list_peers
from proxystore.endpoint.commands import remove_endpoint
from proxystore.endpoint.commands import remove_peer
from proxystore.endpoint.commands import start_endpoint
from proxystore.endpoint.commands import stop_endpoint
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir
from testing.endpoint import random_endpoint_id

_NAME = 'default'
_ID = random_endpoint_id()
_PORT = 1234


@pytest.fixture
def _patch_hostname() -> Generator[None, None, None]:
    # Tests which call start_endpoint will sometimes fail on MacOS
    # in the call to socket.gethostbyname(utils.hostname()).
    # This is commonly because there is no entry in /etc/hosts which matches
    # the hostname returned by proxystore.utils.environment.hostname.
    # This fixture mocks the resulting address to be localhost.
    #
    # Related:
    #   - https://apple.stackexchange.com/a/253834
    #   - https://stackoverflow.com/a/43549848
    # socket.getfqdn() is similarly mocked because it performs a reverse
    # DNS lookup (gethostbyaddr) that can hang on MacOS runners with a
    # .local hostname. See: https://github.com/actions/setup-python/issues/1223
    with (
        mock.patch('socket.gethostbyname', return_value='localhost'),
        mock.patch('socket.getfqdn', return_value='localhost'),
    ):
        yield


def test_get_status(tmp_path: pathlib.Path, caplog) -> None:
    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))
    assert not os.path.isdir(endpoint_dir)

    # Returns UNKNOWN if directory does not exist
    assert get_status(_NAME, str(tmp_path)) == EndpointStatus.UNKNOWN
    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        assert get_status(_NAME) == EndpointStatus.UNKNOWN

    os.makedirs(endpoint_dir, exist_ok=True)

    # Returns UNKNOWN if config is not readable
    assert get_status(_NAME, str(tmp_path)) == EndpointStatus.UNKNOWN

    with mock.patch.object(EndpointDir, 'read_config', return_value=None):
        # Returns STOPPED if PID file does not exist
        assert get_status(_NAME, str(tmp_path)) == EndpointStatus.STOPPED

        with open(endpoint_dir.pid_path, 'w') as f:
            f.write('0')

        with mock.patch(
            'proxystore.endpoint.commands.is_own_process'
        ) as mock_exists:
            # Return RUNNING if PID exists
            mock_exists.return_value = True
            assert get_status(_NAME, str(tmp_path)) == EndpointStatus.RUNNING

            # Return HANGING if PID does not exists
            mock_exists.return_value = False
            assert get_status(_NAME, str(tmp_path)) == EndpointStatus.HANGING

        # Return HANGING if PID was reused by another user's process
        with open(endpoint_dir.pid_path, 'w') as f:
            f.write('1234')
        with mock.patch('os.kill', side_effect=PermissionError):
            assert get_status(_NAME, str(tmp_path)) == EndpointStatus.HANGING


def test_wait_for_exit() -> None:
    with (
        mock.patch(
            'proxystore.endpoint.commands.is_own_process',
            side_effect=[True, False],
        ),
        mock.patch('time.sleep') as mock_sleep,
    ):
        assert _wait_for_exit(os.getpid(), timeout=1)
    mock_sleep.assert_called_once()

    with mock.patch(
        'proxystore.endpoint.commands.is_own_process',
        return_value=True,
    ):
        assert not _wait_for_exit(os.getpid(), timeout=0)


def test_configure_endpoint_basic(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 0

    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))
    assert os.path.exists(endpoint_dir)

    cfg = endpoint_dir.read_config()
    assert cfg.name == _NAME
    assert cfg.host is None
    assert cfg.port == _PORT

    assert any(
        cfg.id in record.message and record.levelname == 'INFO'
        for record in caplog.records
    )


def test_configure_endpoint_home_dir(tmp_path: pathlib.Path) -> None:
    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = configure_endpoint(
            name=_NAME,
            port=_PORT,
        )
    assert rv == 0

    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))
    assert os.path.exists(endpoint_dir)


def test_configure_endpoint_invalid_name(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    rv = configure_endpoint(
        name='abc?',
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 1

    assert any('alphanumeric' in record.message for record in caplog.records)


def test_configure_endpoint_already_exists_error(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 0

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 1

    assert any('already exists' in record.message for record in caplog.records)


def test_list_endpoints(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    names = ['ep1', 'ep2', 'ep3']
    # Raise logging level while creating endpoint so we just get logs from
    # list_endpoints()
    with caplog.at_level(logging.CRITICAL):
        for name in names:
            configure_endpoint(
                name=name,
                port=_PORT,
                proxystore_dir=str(tmp_path),
            )

    rv = list_endpoints(proxystore_dir=str(tmp_path))
    assert rv == 0

    assert len(caplog.records) == len(names) + 2
    for name in names:
        assert any(name in record.message for record in caplog.records)


def test_list_endpoints_empty(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = list_endpoints()
    assert rv == 0

    assert len(caplog.records) == 1
    assert 'No valid endpoint configurations' in caplog.records[0].message


def test_remove_endpoint(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    configure_endpoint(
        name=_NAME,
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )
    assert len([c for _, c in EndpointDir.find_all(str(tmp_path))]) == 1

    remove_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert len([c for _, c in EndpointDir.find_all(str(tmp_path))]) == 0

    assert any(
        'Removed endpoint' in record.message for record in caplog.records
    )


def test_remove_endpoints_does_not_exist(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = remove_endpoint(_NAME)
    assert rv == 1

    assert any('does not exist' in record.message for record in caplog.records)


@pytest.mark.parametrize(
    'status',
    (EndpointStatus.RUNNING, EndpointStatus.HANGING),
)
def test_remove_endpoint_running(
    status: EndpointStatus,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    os.makedirs(os.path.join(tmp_path, _NAME), exist_ok=True)

    with (
        mock.patch(
            'proxystore.endpoint.commands.home_dir',
            return_value=str(tmp_path),
        ),
        mock.patch(
            'proxystore.endpoint.commands.get_status',
            return_value=status,
        ),
    ):
        rv = remove_endpoint(_NAME)
    assert rv == 1

    assert any(
        'must be stopped' in record.message for record in caplog.records
    )


@pytest.mark.usefixtures('_patch_hostname')
@pytest.mark.parametrize('host', ('fqdn', 'ip', 'localhost'))
def test_start_endpoint(host: str, tmp_path: pathlib.Path) -> None:
    configure_endpoint(
        name=_NAME,
        port=_PORT,
        host=host,
        proxystore_dir=str(tmp_path),
    )

    cfg = EndpointDir(os.path.join(tmp_path, _NAME)).read_config()
    if host == 'fqdn':
        assert cfg.host is None
        assert cfg.host_type == 'fqdn'
    elif host == 'ip':
        assert cfg.host is None
        assert cfg.host_type == 'ip'
    else:
        assert cfg.host == host
        assert cfg.host_type == 'static'

    with mock.patch('proxystore.endpoint.commands.serve', autospec=True):
        rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_detached(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    configure_endpoint(
        name=_NAME,
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )
    with (
        mock.patch(
            'proxystore.endpoint.commands.serve',
            autospec=True,
        ),
        mock.patch('daemon.DaemonContext', autospec=True),
    ):
        rv = start_endpoint(_NAME, detach=True, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert any('daemon' in record.message for record in caplog.records)


def test_start_endpoint_running(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with (
        mock.patch(
            'proxystore.endpoint.commands.home_dir',
            return_value=str(tmp_path),
        ),
        mock.patch(
            'proxystore.endpoint.commands.get_status',
            return_value=EndpointStatus.RUNNING,
        ),
    ):
        rv = start_endpoint(_NAME)
    assert rv == 1

    assert any(
        'already running' in record.message for record in caplog.records
    )


def test_start_endpoint_does_not_exist(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = start_endpoint(_NAME)
    assert rv == 1

    assert any('does not exist' in record.message for record in caplog.records)


def test_start_endpoint_missing_config(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    os.makedirs(os.path.join(tmp_path, _NAME))
    rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        'does not contain a valid configuration' in record.message
        for record in caplog.records
    )


def test_start_endpoint_bad_config(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))
    os.makedirs(endpoint_dir)
    with open(endpoint_dir.config_path, 'w') as f:
        f.write('not valid toml')

    rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        'Unable to parse' in record.message for record in caplog.records
    )


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_hanging_different_host(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))

    config = EndpointConfig(
        name=_NAME,
        id=_ID,
        host='abcd',
        port=1234,
    )
    endpoint_dir.write_config(config)

    pid_file = endpoint_dir.pid_path
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch(
        'proxystore.endpoint.commands.is_own_process', return_value=False
    ):
        rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        'on a host named abcd' in record.message for record in caplog.records
    )


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_old_pid_file(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.DEBUG)

    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))

    config = EndpointConfig(name=_NAME, id=_ID, host=None, port=1234)
    endpoint_dir.write_config(config)

    pid_file = endpoint_dir.pid_path
    with open(pid_file, 'w') as f:
        f.write('1')

    with (
        mock.patch(
            'proxystore.endpoint.commands.is_own_process', return_value=False
        ),
        mock.patch(
            'proxystore.endpoint.commands.serve',
            autospec=True,
        ),
    ):
        rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert any(
        'Removing invalid PID file' in record.message
        for record in caplog.records
        if record.levelno == logging.DEBUG
    )


def test_start_endpoint_missing_static_host(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)

    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))

    config = EndpointConfig(
        name=_NAME,
        id=_ID,
        host=None,
        host_type='static',
        port=1234,
    )
    endpoint_dir.write_config(config)

    rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        'Missing static host address in config.' in record.message
        for record in caplog.records
        if record.levelno == logging.ERROR
    )


@pytest.mark.timeout(2)
def test_stop_endpoint(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))
    configure_endpoint(
        name=_NAME,
        port=_PORT,
        proxystore_dir=str(tmp_path),
    )

    # Create a fake process to kill
    context = multiprocessing.get_context('spawn')
    p = context.Process(target=time.sleep, args=(1000,))
    p.start()

    pid_file = endpoint_dir.pid_path
    with open(pid_file, 'w') as f:
        f.write(str(p.pid))

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = stop_endpoint(_NAME)
    assert rv == 0
    assert not os.path.exists(pid_file)

    # Process was terminated so this should happen immediately
    p.join()


def test_stop_endpoint_unknown(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    with mock.patch(
        'proxystore.endpoint.commands.get_status',
        return_value=EndpointStatus.UNKNOWN,
    ):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any('does not exist' in record.message for record in caplog.records)


def test_stop_endpoint_not_running(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    with mock.patch(
        'proxystore.endpoint.commands.get_status',
        return_value=EndpointStatus.STOPPED,
    ):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert any('not running' in record.message for record in caplog.records)


def test_stop_endpoint_hanging_different_host(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))

    config = EndpointConfig(
        name=_NAME,
        id=_ID,
        host='abcd',
        port=1234,
    )
    endpoint_dir.write_config(config)

    pid_file = endpoint_dir.pid_path
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch(
        'proxystore.endpoint.commands.is_own_process', return_value=False
    ):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        'on a host named abcd' in record.message for record in caplog.records
    )


def test_stop_endpoint_dangling_pid_file(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    endpoint_dir = EndpointDir(os.path.join(tmp_path, _NAME))

    config = EndpointConfig(name=_NAME, id=_ID, host=None, port=1234)
    endpoint_dir.write_config(config)

    pid_file = endpoint_dir.pid_path
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch(
        'proxystore.endpoint.commands.is_own_process', return_value=False
    ):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert not os.path.exists(pid_file)

    assert any(
        'Removing invalid PID file' in record.message
        for record in caplog.records
        if record.levelno == logging.DEBUG
    )
    assert any(
        'not running' in record.message
        for record in caplog.records
        if record.levelno == logging.INFO
    )


def _configure(tmp_path: pathlib.Path, name: str = _NAME) -> EndpointConfig:
    assert (
        configure_endpoint(name, port=_PORT, proxystore_dir=str(tmp_path)) == 0
    )
    return EndpointDir.from_home(str(tmp_path), name).read_config()


def test_get_endpoint_id(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    config = _configure(tmp_path)
    caplog.clear()

    assert get_endpoint_id(_NAME, proxystore_dir=str(tmp_path)) == 0
    assert caplog.records[-1].message == config.id

    assert get_endpoint_id('missing', proxystore_dir=str(tmp_path)) == 1
    assert 'does not exist' in caplog.records[-1].message


def test_get_endpoint_id_bad_config(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)
    os.makedirs(tmp_path / _NAME)
    assert get_endpoint_id(_NAME, proxystore_dir=str(tmp_path)) == 1
    assert 'valid configuration' in caplog.records[-1].message


def test_add_list_remove_peer(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    home = str(tmp_path)
    _configure(tmp_path)
    peer_id = random_endpoint_id()

    assert list_peers(_NAME, proxystore_dir=home) == 0
    assert any('has no peers' in r.message for r in caplog.records)

    assert add_peer(_NAME, 'peer', peer_id, proxystore_dir=home) == 0
    endpoint_dir = EndpointDir.from_home(home, _NAME)
    assert endpoint_dir.read_peers().peers == {'peer': peer_id}

    caplog.clear()
    assert list_peers(_NAME, proxystore_dir=home) == 0
    assert caplog.records[-1].message.split() == ['peer', peer_id]

    assert remove_peer(_NAME, 'peer', proxystore_dir=home) == 0
    assert endpoint_dir.read_peers().peers == {}
    assert remove_peer(_NAME, 'peer', proxystore_dir=home) == 1
    assert 'no peer named peer' in caplog.records[-1].message


def test_add_peer_errors(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)
    home = str(tmp_path)
    config = _configure(tmp_path)
    peer_id = random_endpoint_id()
    assert add_peer(_NAME, 'peer', peer_id, proxystore_dir=home) == 0

    def _error(*args: str) -> str:
        assert add_peer(*args, proxystore_dir=home) == 1
        return caplog.records[-1].message

    assert 'does not exist' in _error('missing', 'p', peer_id)
    assert 'alphanumeric' in _error(_NAME, 'bad name', peer_id)
    assert 'not a valid endpoint ID' in _error(_NAME, 'p', 'xyz')
    assert 'not a valid public key' in _error(_NAME, 'p', '02' * 32)
    assert 'peer of itself' in _error(_NAME, 'p', config.id)
    caplog.clear()
    _error(_NAME, 'peer', random_endpoint_id())
    assert 'already exists' in caplog.records[0].message
    assert 'already a peer named peer' in _error(_NAME, 'p', peer_id)


def test_peer_commands_malformed_peers(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)
    home = str(tmp_path)
    _configure(tmp_path)
    endpoint_dir = EndpointDir.from_home(home, _NAME)
    with open(endpoint_dir.peers_path, 'w') as f:
        f.write('not toml')

    peer_id = random_endpoint_id()
    assert add_peer(_NAME, 'peer', peer_id, proxystore_dir=home) == 1
    assert remove_peer(_NAME, 'peer', proxystore_dir=home) == 1
    assert list_peers(_NAME, proxystore_dir=home) == 1
    assert all('Unable to parse' in r.message for r in caplog.records)


@pytest.mark.parametrize('command', (remove_peer, list_peers))
def test_peer_commands_missing_endpoint(
    command: Any,
    tmp_path: pathlib.Path,
) -> None:
    args = ('missing', 'peer') if command is remove_peer else ('missing',)
    assert command(*args, proxystore_dir=str(tmp_path)) == 1


def test_peer_commands_default_home(tmp_path: pathlib.Path) -> None:
    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        assert get_endpoint_id('missing') == 1

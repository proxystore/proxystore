from __future__ import annotations

import logging
import os
import pathlib
import time
import uuid
from multiprocessing import Process
from typing import Generator
from unittest import mock

import pytest

from proxystore.endpoint.commands import configure_endpoint
from proxystore.endpoint.commands import EndpointStatus
from proxystore.endpoint.commands import get_status
from proxystore.endpoint.commands import list_endpoints
from proxystore.endpoint.commands import remove_endpoint
from proxystore.endpoint.commands import start_endpoint
from proxystore.endpoint.commands import stop_endpoint
from proxystore.endpoint.config import ENDPOINT_CONFIG_FILE
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import get_pid_filepath
from proxystore.endpoint.config import read_config
from proxystore.endpoint.config import write_config

_NAME = 'default'
_UUID = uuid.uuid4()
_PORT = 1234
_SERVER = None


@pytest.fixture()
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
    with mock.patch('socket.gethostbyname', return_value='localhost'):
        yield


def test_get_status(tmp_path: pathlib.Path, caplog) -> None:
    endpoint_dir = os.path.join(tmp_path, _NAME)
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

    with mock.patch(
        'proxystore.endpoint.commands.read_config',
        return_value=None,
    ):
        # Returns STOPPED if PID file does not exist
        assert get_status(_NAME, str(tmp_path)) == EndpointStatus.STOPPED

        with open(get_pid_filepath(endpoint_dir), 'w') as f:
            f.write('0')

        with mock.patch('psutil.pid_exists') as mock_exists:
            # Return RUNNING if PID exists
            mock_exists.return_value = True
            assert get_status(_NAME, str(tmp_path)) == EndpointStatus.RUNNING

            # Return HANGING if PID does not exists
            mock_exists.return_value = False
            assert get_status(_NAME, str(tmp_path)) == EndpointStatus.HANGING


def test_configure_endpoint_basic(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 0

    endpoint_dir = os.path.join(tmp_path, _NAME)
    assert os.path.exists(endpoint_dir)

    cfg = read_config(endpoint_dir)
    assert cfg.name == _NAME
    assert cfg.host is None
    assert cfg.port == _PORT
    assert cfg.relay.address == _SERVER

    assert any(
        [
            str(cfg.uuid) in record.message and record.levelname == 'INFO'
            for record in caplog.records
        ],
    )


def test_configure_endpoint_home_dir(tmp_path: pathlib.Path) -> None:
    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = configure_endpoint(
            name=_NAME,
            port=_PORT,
            relay_server=_SERVER,
        )
    assert rv == 0

    endpoint_dir = os.path.join(tmp_path, _NAME)
    assert os.path.exists(endpoint_dir)


def test_configure_endpoint_invalid_name(caplog) -> None:
    caplog.set_level(logging.ERROR)

    rv = configure_endpoint(
        name='abc?',
        port=_PORT,
        relay_server=_SERVER,
    )
    assert rv == 1

    assert any(['alphanumeric' in record.message for record in caplog.records])


def test_configure_endpoint_already_exists_error(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 0

    rv = configure_endpoint(
        name=_NAME,
        port=_PORT,
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )
    assert rv == 1

    assert any(
        ['already exists' in record.message for record in caplog.records],
    )


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
                relay_server=_SERVER,
                proxystore_dir=str(tmp_path),
            )

    rv = list_endpoints(proxystore_dir=str(tmp_path))
    assert rv == 0

    assert len(caplog.records) == len(names) + 2
    for name in names:
        assert any([name in record.message for record in caplog.records])


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
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )
    assert len(get_configs(str(tmp_path))) == 1

    remove_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert len(get_configs(str(tmp_path))) == 0

    assert any(
        ['Removed endpoint' in record.message for record in caplog.records],
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

    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


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

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ), mock.patch(
        'proxystore.endpoint.commands.get_status',
        return_value=status,
    ):
        rv = remove_endpoint(_NAME)
    assert rv == 1

    assert any(
        ['must be stopped' in record.message for record in caplog.records],
    )


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint(tmp_path: pathlib.Path) -> None:
    configure_endpoint(
        name=_NAME,
        port=_PORT,
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )
    with mock.patch('proxystore.endpoint.commands.serve', autospec=True):
        rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_detached(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)

    configure_endpoint(
        name=_NAME,
        port=_PORT,
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )
    with mock.patch(
        'proxystore.endpoint.commands.serve',
        autospec=True,
    ), mock.patch('daemon.DaemonContext', autospec=True):
        rv = start_endpoint(_NAME, detach=True, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert any(['daemon' in record.message for record in caplog.records])


def test_start_endpoint_running(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ), mock.patch(
        'proxystore.endpoint.commands.get_status',
        return_value=EndpointStatus.RUNNING,
    ):
        rv = start_endpoint(_NAME)
    assert rv == 1

    assert any(
        ['already running' in record.message for record in caplog.records],
    )


def test_start_endpoint_does_not_exist(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        'proxystore.endpoint.commands.home_dir',
        return_value=str(tmp_path),
    ):
        rv = start_endpoint(_NAME)
    assert rv == 1

    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_start_endpoint_missing_config(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    os.makedirs(os.path.join(tmp_path, _NAME))
    rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        [
            'does not contain a valid configuration' in record.message
            for record in caplog.records
        ],
    )


def test_start_endpoint_bad_config(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.ERROR)

    endpoint_dir = os.path.join(tmp_path, _NAME)
    os.makedirs(endpoint_dir)
    with open(os.path.join(endpoint_dir, ENDPOINT_CONFIG_FILE), 'w') as f:
        f.write('not valid toml')

    rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        ['Unable to parse' in record.message for record in caplog.records],
    )


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_hanging_different_host(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)

    endpoint_dir = os.path.join(tmp_path, _NAME)

    config = EndpointConfig(
        name=_NAME,
        uuid=str(_UUID),
        host='abcd',
        port=1234,
    )
    write_config(config, endpoint_dir)

    pid_file = get_pid_filepath(endpoint_dir)
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch('psutil.pid_exists', return_value=False):
        rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        [
            'on a host named abcd' in record.message
            for record in caplog.records
        ],
    )


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_old_pid_file(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.DEBUG)

    endpoint_dir = os.path.join(tmp_path, _NAME)

    config = EndpointConfig(name=_NAME, uuid=str(_UUID), host=None, port=1234)
    write_config(config, endpoint_dir)

    pid_file = get_pid_filepath(endpoint_dir)
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch('psutil.pid_exists', return_value=False), mock.patch(
        'proxystore.endpoint.commands.serve',
        autospec=True,
    ):
        rv = start_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert any(
        [
            'Removing invalid PID file' in record.message
            for record in caplog.records
            if record.levelno == logging.DEBUG
        ],
    )


@pytest.mark.timeout(2)
def test_stop_endpoint(tmp_path: pathlib.Path) -> None:
    endpoint_dir = os.path.join(tmp_path, _NAME)
    configure_endpoint(
        name=_NAME,
        port=_PORT,
        relay_server=_SERVER,
        proxystore_dir=str(tmp_path),
    )

    # Create a fake process to kill
    p = Process(target=time.sleep, args=(1000,))
    p.start()

    pid_file = get_pid_filepath(endpoint_dir)
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

    assert any(
        ['does not exist' in record.message for record in caplog.records],
    )


def test_stop_endpoint_not_running(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    with mock.patch(
        'proxystore.endpoint.commands.get_status',
        return_value=EndpointStatus.STOPPED,
    ):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert any(['not running' in record.message for record in caplog.records])


def test_stop_endpoint_hanging_different_host(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    endpoint_dir = os.path.join(tmp_path, _NAME)

    config = EndpointConfig(
        name=_NAME,
        uuid=str(_UUID),
        host='abcd',
        port=1234,
    )
    write_config(config, endpoint_dir)

    pid_file = get_pid_filepath(endpoint_dir)
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch('psutil.pid_exists', return_value=False):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 1

    assert any(
        [
            'on a host named abcd' in record.message
            for record in caplog.records
        ],
    )


def test_stop_endpoint_dangling_pid_file(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    endpoint_dir = os.path.join(tmp_path, _NAME)

    config = EndpointConfig(name=_NAME, uuid=str(_UUID), host=None, port=1234)
    write_config(config, endpoint_dir)

    pid_file = get_pid_filepath(endpoint_dir)
    with open(pid_file, 'w') as f:
        f.write('1')

    with mock.patch('psutil.pid_exists', return_value=False):
        rv = stop_endpoint(_NAME, proxystore_dir=str(tmp_path))
    assert rv == 0

    assert not os.path.exists(pid_file)

    assert any(
        [
            'Removing invalid PID file' in record.message
            for record in caplog.records
            if record.levelno == logging.DEBUG
        ],
    )
    assert any(
        [
            'not running' in record.message
            for record in caplog.records
            if record.levelno == logging.INFO
        ],
    )

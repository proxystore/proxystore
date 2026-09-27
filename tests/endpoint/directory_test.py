from __future__ import annotations

import json
import os
import pathlib
import stat
import subprocess
import sys
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.directory import resolve_home
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.identity import SecretKey


def test_endpoint_dir_paths() -> None:
    endpoint_dir = EndpointDir('/path/to/endpoint')
    assert os.fspath(endpoint_dir) == str(endpoint_dir) == '/path/to/endpoint'

    paths = [
        endpoint_dir.config_path,
        endpoint_dir.log_path,
        endpoint_dir.pid_path,
        endpoint_dir.connection_path,
        endpoint_dir.secret_key_path,
    ]
    assert all(os.path.dirname(p) == '/path/to/endpoint' for p in paths)
    assert len(set(paths)) == len(paths)


@pytest.mark.parametrize(
    ('mode', 'expected'),
    ((0o700, 0o700), (0o755, 0o700), (0o775, 0o700), (0o777, 0o700)),
)
def test_restrict_permissions(
    mode: int,
    expected: int,
    tmp_path: pathlib.Path,
) -> None:
    os.chmod(tmp_path, mode)
    changed = EndpointDir(str(tmp_path)).restrict_permissions()
    assert changed == (mode != expected)
    assert stat.S_IMODE(os.stat(tmp_path).st_mode) == expected


def test_restrict_permissions_secret_key(tmp_path: pathlib.Path) -> None:
    os.chmod(tmp_path, 0o700)
    endpoint_dir = EndpointDir(str(tmp_path))
    endpoint_dir.write_secret_key(SecretKey.generate())
    assert not endpoint_dir.restrict_permissions()

    os.chmod(endpoint_dir.secret_key_path, 0o644)
    assert endpoint_dir.restrict_permissions()
    mode = stat.S_IMODE(os.stat(endpoint_dir.secret_key_path).st_mode)
    assert mode == 0o600


def test_secret_key_read_write(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    with pytest.raises(EndpointConfigError):
        endpoint_dir.read_secret_key()

    secret_key = SecretKey.generate()
    endpoint_dir.write_secret_key(secret_key)
    assert endpoint_dir.read_secret_key() == secret_key
    mode = stat.S_IMODE(os.stat(endpoint_dir.secret_key_path).st_mode)
    assert mode == 0o600

    with open(endpoint_dir.secret_key_path, 'wb') as f:
        f.write(b'abc')
    with pytest.raises(ValueError, match='malformed'):
        endpoint_dir.read_secret_key()


@pytest.mark.parametrize('fingerprint', ('abcd', None))
def test_connection_lifecycle(
    fingerprint: str | None,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        endpoint_dir.read_connection()

    info = _connection_info(tls_fingerprint=fingerprint)
    endpoint_dir.write_connection(info)
    mode = stat.S_IMODE(os.stat(endpoint_dir.connection_path).st_mode)
    assert mode == 0o600
    assert endpoint_dir.read_connection() == info

    endpoint_dir.remove_connection()
    assert os.listdir(tmp_path) == []
    # Removing again is a no-op
    endpoint_dir.remove_connection()


def test_remove_connection_only_if_matches(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    ours = _connection_info()
    theirs = _connection_info()

    # No file to remove
    endpoint_dir.remove_connection(ours)

    endpoint_dir.write_connection(theirs)
    endpoint_dir.remove_connection(ours)
    assert endpoint_dir.read_connection() == theirs

    endpoint_dir.remove_connection(theirs)
    assert not os.path.exists(endpoint_dir.connection_path)


def _connection_info(**kwargs: Any) -> ConnectionInfo:
    options: dict[str, Any] = {
        'host': 'localhost',
        'port': 1234,
        'token': EndpointToken.generate(),
        'tls_fingerprint': None,
        'hostname': 'machine',
        'pid': 42,
        **kwargs,
    }
    return ConnectionInfo(**options)


def _connection_data(**kwargs: Any) -> dict[str, Any]:
    return {
        'version': 1,
        'host': 'h',
        'port': 1,
        'token': EndpointToken.generate().hex(),
        'tls_fingerprint': None,
        'hostname': 'machine',
        'pid': 42,
        **kwargs,
    }


@pytest.mark.parametrize(
    'contents',
    (
        'not json',
        '[]',
        json.dumps({'version': 1}),
        json.dumps(_connection_data(token='zz')),
        json.dumps(_connection_data(token='abcd')),
        json.dumps(_connection_data(host=1)),
        json.dumps(_connection_data(port='1')),
        json.dumps(_connection_data(tls_fingerprint=1)),
        json.dumps(_connection_data(hostname=None)),
        json.dumps(_connection_data(pid='42')),
    ),
)
def test_read_connection_malformed(
    contents: str,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    with open(endpoint_dir.connection_path, 'w') as f:
        f.write(contents)
    with pytest.raises(ValueError, match='malformed'):
        endpoint_dir.read_connection()


@pytest.mark.parametrize('version', (None, 0, 2))
def test_read_connection_unsupported_version(
    version: Any,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    data = _connection_data(version=version)
    if version is None:
        data.pop('version')
    with open(endpoint_dir.connection_path, 'w') as f:
        json.dump(data, f)
    with pytest.raises(ValueError, match='only supports version 1'):
        endpoint_dir.read_connection()


def test_is_own_process() -> None:
    assert is_own_process(os.getpid())
    assert not is_own_process(0)
    assert not is_own_process(-1)

    # Use a plain subprocess because, under coverage, a multiprocessing child
    # that runs no measured code warns that no data was collected.
    p = subprocess.Popen([sys.executable, '-c', ''])
    p.wait()
    assert not is_own_process(p.pid)

    with mock.patch('os.kill', side_effect=PermissionError):
        assert not is_own_process(os.getpid())


def test_running_pid(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    assert endpoint_dir.running_pid() is None

    with open(endpoint_dir.pid_path, 'w') as f:
        f.write('not-a-pid')
    assert endpoint_dir.running_pid() is None

    with open(endpoint_dir.pid_path, 'w') as f:
        f.write(f'{os.getpid()}\n')
    assert endpoint_dir.running_pid() == os.getpid()

    with mock.patch(
        'proxystore.endpoint.directory.is_own_process',
        return_value=False,
    ):
        assert endpoint_dir.running_pid() is None


def test_create(tmp_path: pathlib.Path) -> None:
    home = str(tmp_path / 'home')
    endpoint_dir = EndpointDir.create('my-ep', home, port=1234)
    assert endpoint_dir == EndpointDir.from_name('my-ep', home)
    assert stat.S_IMODE(os.stat(endpoint_dir.path).st_mode) == 0o700

    config = endpoint_dir.read_config()
    assert config.name == 'my-ep'
    assert config.port == 1234
    assert endpoint_dir.read_secret_key().endpoint_id == config.id
    assert [d for d, _ in EndpointDir.find_all(home)] == [endpoint_dir]


def test_create_with_secret_key(tmp_path: pathlib.Path) -> None:
    secret_key = SecretKey.generate()
    endpoint_dir = EndpointDir.create(
        'my-ep',
        str(tmp_path),
        secret_key=secret_key,
        port=1234,
    )
    assert endpoint_dir.read_config().id == secret_key.endpoint_id
    assert endpoint_dir.read_secret_key() == secret_key


def test_create_existing(tmp_path: pathlib.Path) -> None:
    EndpointDir.create('my-ep', str(tmp_path), port=1234)
    with pytest.raises(FileExistsError, match='already exists'):
        EndpointDir.create('my-ep', str(tmp_path), port=1234)


def test_create_invalid_config(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match='Port must be in range'):
        EndpointDir.create('my-ep', str(tmp_path), port=0)
    # Nothing is written if the configuration is invalid
    assert not os.path.exists(tmp_path / 'my-ep')


def test_default_home(tmp_path: pathlib.Path) -> None:
    with mock.patch(
        'proxystore.endpoint.directory.home_dir',
        return_value=str(tmp_path),
    ):
        assert resolve_home() == str(tmp_path)
        assert resolve_home('/other') == '/other'
        endpoint_dir = EndpointDir.create('my-ep', port=1234)
        assert endpoint_dir.path == str(tmp_path / 'my-ep')
        assert EndpointDir.from_name('my-ep') == endpoint_dir
        assert [d for d, _ in EndpointDir.find_all()] == [endpoint_dir]


def test_read_secret_key_mismatch(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('my-ep', str(tmp_path), port=1234)
    endpoint_dir.write_secret_key(SecretKey.generate())
    with pytest.raises(ValueError, match='does not match the secret key'):
        endpoint_dir.read_secret_key()


def test_read_secret_key_missing(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('my-ep', str(tmp_path), port=1234)
    os.remove(endpoint_dir.secret_key_path)
    with pytest.raises(EndpointConfigError, match='configure it again'):
        endpoint_dir.read_secret_key()


def test_resolve_path(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path / 'my-ep'))
    assert endpoint_dir.resolve_path('blobs.db') == str(
        tmp_path / 'my-ep' / 'blobs.db',
    )
    assert endpoint_dir.resolve_path('/lustre/blobs.db') == '/lustre/blobs.db'
    assert endpoint_dir.resolve_path('~/blobs.db') == os.path.expanduser(
        '~/blobs.db',
    )


def test_errors_are_endpoint_errors(tmp_path: pathlib.Path) -> None:
    # Errors are EndpointErrors and the equivalent built-in exception
    with pytest.raises(EndpointNotFoundError) as not_found:
        EndpointDir.from_name('missing', str(tmp_path)).read_config()
    assert isinstance(not_found.value, EndpointError)
    assert isinstance(not_found.value, FileNotFoundError)

    EndpointDir.create('ep', str(tmp_path), port=1234)
    with pytest.raises(EndpointExistsError) as exists:
        EndpointDir.create('ep', str(tmp_path), port=1234)
    assert isinstance(exists.value, EndpointError)
    assert isinstance(exists.value, FileExistsError)

    endpoint_dir = EndpointDir.from_name('ep', str(tmp_path))
    with open(endpoint_dir.config_path, 'w') as f:
        f.write('not toml')
    with pytest.raises(EndpointConfigError) as invalid:
        endpoint_dir.read_config()
    assert isinstance(invalid.value, EndpointError)
    assert isinstance(invalid.value, ValueError)


def test_status(tmp_path: pathlib.Path, caplog) -> None:
    endpoint_dir = EndpointDir(os.path.join(tmp_path, 'ep'))
    assert not os.path.isdir(endpoint_dir)

    # Returns UNKNOWN if directory does not exist
    assert endpoint_dir.status() == EndpointStatus.UNKNOWN

    os.makedirs(endpoint_dir, exist_ok=True)

    # Returns UNKNOWN if config is not readable
    assert endpoint_dir.status() == EndpointStatus.UNKNOWN

    with mock.patch.object(EndpointDir, 'read_config', return_value=None):
        # Returns STOPPED if PID file does not exist
        assert endpoint_dir.status() == EndpointStatus.STOPPED

        with open(endpoint_dir.pid_path, 'w') as f:
            f.write('0')

        with mock.patch(
            'proxystore.endpoint.directory.is_own_process'
        ) as mock_exists:
            # Return RUNNING if PID exists
            mock_exists.return_value = True
            assert endpoint_dir.status() == EndpointStatus.RUNNING

            # Return HANGING if PID does not exists
            mock_exists.return_value = False
            assert endpoint_dir.status() == EndpointStatus.HANGING

        # Return HANGING if PID was reused by another user's process
        with open(endpoint_dir.pid_path, 'w') as f:
            f.write('1234')
        with mock.patch('os.kill', side_effect=PermissionError):
            assert endpoint_dir.status() == EndpointStatus.HANGING

        # Return HANGING rather than raising if the PID file is malformed
        with open(endpoint_dir.pid_path, 'w') as f:
            f.write('not a pid')
        assert endpoint_dir.status() == EndpointStatus.HANGING


def test_remove(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    endpoint_dir.remove()
    assert not os.path.exists(endpoint_dir.path)
    with pytest.raises(EndpointNotFoundError, match='does not exist'):
        endpoint_dir.remove()


@pytest.mark.parametrize(
    'status',
    (EndpointStatus.RUNNING, EndpointStatus.HANGING),
)
def test_remove_running(
    status: EndpointStatus, tmp_path: pathlib.Path
) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    with (
        mock.patch.object(EndpointDir, 'status', return_value=status),
        pytest.raises(EndpointRunningError, match='must be stopped'),
    ):
        endpoint_dir.remove()
    assert os.path.exists(endpoint_dir.path)


def test_create_random_port(tmp_path: pathlib.Path) -> None:
    config = EndpointDir.create('ep', str(tmp_path)).read_config()
    assert 10 * 1024 <= config.port <= 20 * 1024

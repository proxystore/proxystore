from __future__ import annotations

import errno
import fcntl
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
from proxystore.endpoint.directory import EndpointLock
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.identity import SecretKey
from proxystore.utils.environment import hostname


def test_endpoint_dir_paths() -> None:
    endpoint_dir = EndpointDir('/path/to/endpoint')
    assert os.fspath(endpoint_dir) == str(endpoint_dir) == '/path/to/endpoint'
    assert endpoint_dir.name == EndpointDir('/path/to/endpoint/').name
    assert endpoint_dir.name == 'endpoint'

    paths = [
        endpoint_dir.config_path,
        endpoint_dir.log_path,
        endpoint_dir.lock_path,
        endpoint_dir.peers_path,
        endpoint_dir.peer_addrs_path,
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
    secret_key = SecretKey.generate()
    with pytest.raises(EndpointConfigError):
        endpoint_dir.read_secret_key(secret_key.endpoint_id)

    endpoint_dir.write_secret_key(secret_key)
    assert endpoint_dir.read_secret_key(secret_key.endpoint_id) == secret_key
    mode = stat.S_IMODE(os.stat(endpoint_dir.secret_key_path).st_mode)
    assert mode == 0o600

    with open(endpoint_dir.secret_key_path, 'wb') as f:
        f.write(b'abc')
    with pytest.raises(ValueError, match='malformed'):
        endpoint_dir.read_secret_key(secret_key.endpoint_id)


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


def test_lock(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    lock = endpoint_dir.lock()
    assert lock.path == endpoint_dir.lock_path
    assert not lock.held
    # The lock file does not exist yet
    assert lock.is_locked() is False

    lock.acquire()
    assert lock.held
    assert stat.S_IMODE(os.stat(lock.path).st_mode) == 0o600
    # The lock conflicts with other lock objects, even in this process
    other = endpoint_dir.lock()
    assert other.is_locked()
    with (
        mock.patch.object(EndpointLock, '_ACQUIRE_TIMEOUT', 0),
        pytest.raises(EndpointRunningError, match='already running'),
    ):
        other.acquire()
    assert not other.held
    with pytest.raises(RuntimeError, match='already held'):
        lock.acquire()

    lock.release()
    assert not lock.held
    assert other.is_locked() is False
    other.acquire()
    other.release()
    # Releasing a lock which is not held is a no-op
    other.release()


def test_lock_concurrent_check(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    lock = endpoint_dir.lock()
    # Simulate another thread or process checking if the lock is held.
    fd = os.open(lock.path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        # Concurrent checks do not see each other as the endpoint.
        assert lock.is_locked() is False
        # Acquiring is retried until the check finishes, which happens
        # while acquire() waits to retry.
        with mock.patch(
            'proxystore.endpoint.directory.time.sleep',
            side_effect=lambda _: fcntl.flock(fd, fcntl.LOCK_UN),
        ) as sleep:
            lock.acquire()
        sleep.assert_called_once()
        assert lock.held
    finally:
        os.close(fd)
    lock.release()


def test_lock_released_when_process_exits(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    code = (
        'import sys; '
        'from proxystore.endpoint.directory import EndpointDir; '
        f'EndpointDir({str(tmp_path)!r}).lock().acquire(); '
        'sys.stdout.write("locked\\n"); sys.stdout.flush(); '
        'sys.stdin.read()'
    )
    process = subprocess.Popen(
        [sys.executable, '-c', code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    assert process.stdout is not None
    assert process.stdout.readline() == b'locked\n'
    assert endpoint_dir.lock().is_locked()
    process.kill()
    process.wait()
    assert endpoint_dir.lock().is_locked() is False


@pytest.mark.parametrize('errno_', (errno.ENOLCK, errno.EOPNOTSUPP))
def test_lock_unsupported(
    errno_: int,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    lock = endpoint_dir.lock()
    with mock.patch('fcntl.flock', side_effect=OSError(errno_, 'no locks')):
        lock.acquire()
        assert lock.held
        assert not lock.supported
        assert endpoint_dir.lock().is_locked() is None
    lock.release()
    assert any('does not support locks' in r.message for r in caplog.records)


def test_lock_error(tmp_path: pathlib.Path) -> None:
    lock = EndpointDir(str(tmp_path)).lock()
    with (
        mock.patch('fcntl.flock', side_effect=OSError(errno.EIO, 'io')),
        pytest.raises(OSError, match='io'),
    ):
        lock.acquire()
    assert not lock.held


def test_create(tmp_path: pathlib.Path) -> None:
    home = str(tmp_path / 'home')
    endpoint_dir = EndpointDir.create('my-ep', home, port=1234)
    assert endpoint_dir == EndpointDir.from_name('my-ep', home)
    assert stat.S_IMODE(os.stat(endpoint_dir.path).st_mode) == 0o700
    config_mode = os.stat(endpoint_dir.config_path).st_mode
    assert stat.S_IMODE(config_mode) == 0o600

    config = endpoint_dir.read_config()
    assert config.name == 'my-ep'
    assert config.port == 1234
    assert endpoint_dir.read_secret_key(config.id).endpoint_id == config.id
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
    assert endpoint_dir.read_secret_key(secret_key.endpoint_id) == secret_key


def test_create_invalid_config(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match='Port must be in range'):
        EndpointDir.create('my-ep', str(tmp_path), port=0)
    # Nothing is written if the configuration is invalid
    assert not os.path.exists(tmp_path / 'my-ep')


def test_default_home(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('PROXYSTORE_HOME', str(tmp_path))
    endpoint_dir = EndpointDir.create('my-ep', port=1234)
    assert endpoint_dir.path == str(tmp_path / 'my-ep')
    assert EndpointDir.from_name('my-ep') == endpoint_dir
    assert [d for d, _ in EndpointDir.find_all()] == [endpoint_dir]


def test_read_secret_key_mismatch(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('my-ep', str(tmp_path), port=1234)
    endpoint_dir.write_secret_key(SecretKey.generate())
    with pytest.raises(ValueError, match='does not match the secret key'):
        endpoint_dir.read_secret_key(endpoint_dir.read_config().id)


def test_read_secret_key_missing(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('my-ep', str(tmp_path), port=1234)
    os.remove(endpoint_dir.secret_key_path)
    with pytest.raises(EndpointConfigError, match='configure it again'):
        endpoint_dir.read_secret_key(endpoint_dir.read_config().id)


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


def test_status_missing(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(os.path.join(tmp_path, 'ep'))
    with pytest.raises(EndpointNotFoundError):
        endpoint_dir.status()
    # The status does not depend on the configuration
    os.makedirs(endpoint_dir)
    assert endpoint_dir.status() == EndpointStatus.STOPPED


def test_status(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    assert endpoint_dir.status() == EndpointStatus.STOPPED

    lock = endpoint_dir.lock()
    lock.acquire()
    # The endpoint holds its lock before writing its connection file
    assert endpoint_dir.status() == EndpointStatus.RUNNING
    endpoint_dir.write_connection(_connection_info(hostname=hostname()))
    assert endpoint_dir.status() == EndpointStatus.RUNNING
    lock.release()

    # The endpoint stopped without removing its connection file
    assert endpoint_dir.status() == EndpointStatus.STALE
    with open(endpoint_dir.connection_path, 'w') as f:
        f.write('not json')
    assert endpoint_dir.status() == EndpointStatus.STALE

    endpoint_dir.write_connection(_connection_info(hostname='other'))
    assert endpoint_dir.status() == EndpointStatus.OTHER_HOST
    # The lock of an endpoint on another host may be visible to this host
    lock.acquire()
    assert endpoint_dir.status() == EndpointStatus.OTHER_HOST
    lock.release()

    # The lock is held but the connection file cannot be read
    with open(endpoint_dir.connection_path, 'w') as f:
        f.write('not json')
    lock.acquire()
    assert endpoint_dir.status() == EndpointStatus.RUNNING
    lock.release()


def test_status_locks_unsupported(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    with mock.patch.object(EndpointLock, 'is_locked', return_value=None):
        assert endpoint_dir.status() == EndpointStatus.STOPPED

        info = _connection_info(hostname=hostname(), pid=os.getpid())
        endpoint_dir.write_connection(info)
        # The PID of the connection file is used instead of the lock
        assert endpoint_dir.status() == EndpointStatus.RUNNING
        with mock.patch(
            'proxystore.endpoint.directory.is_own_process',
            return_value=False,
        ):
            assert endpoint_dir.status() == EndpointStatus.STALE


def test_check_stopped(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    endpoint_dir.check_stopped()

    # A stale connection file is removed
    endpoint_dir.write_connection(_connection_info(hostname=hostname()))
    endpoint_dir.check_stopped()
    assert not os.path.exists(endpoint_dir.connection_path)

    lock = endpoint_dir.lock()
    lock.acquire()
    with pytest.raises(EndpointRunningError, match='already running'):
        endpoint_dir.check_stopped()
    lock.release()

    endpoint_dir.write_connection(_connection_info(hostname='other', pid=7))
    with pytest.raises(
        EndpointRunningError,
        match=r'running on other \(PID 7\)',
    ):
        endpoint_dir.check_stopped()
    assert os.path.exists(endpoint_dir.connection_path)


def test_remove(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    endpoint_dir.remove()
    assert not os.path.exists(endpoint_dir.path)
    with pytest.raises(
        EndpointNotFoundError,
        match=f'An endpoint named ep does not exist in {tmp_path}',
    ):
        endpoint_dir.remove()


def test_remove_running(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    lock = endpoint_dir.lock()
    lock.acquire()
    try:
        with pytest.raises(EndpointRunningError, match='already running'):
            endpoint_dir.remove()
    finally:
        lock.release()
    assert os.path.exists(endpoint_dir.path)


def test_create_random_port(tmp_path: pathlib.Path) -> None:
    config = EndpointDir.create('ep', str(tmp_path)).read_config()
    assert 10 * 1024 <= config.port <= 20 * 1024

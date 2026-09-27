from __future__ import annotations

import contextlib
import logging
import multiprocessing
import os
import pathlib
import subprocess
import sys
import threading
from collections.abc import Generator
from unittest import mock

import pytest

from proxystore import utils
from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.directory import is_own_process
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.process import _wait_for_exit
from proxystore.endpoint.process import configure_logging
from proxystore.endpoint.process import serve
from proxystore.endpoint.process import start_endpoint
from proxystore.endpoint.process import stop_endpoint
from testing.endpoint import terminate_process
from testing.endpoint import wait_for_endpoint
from testing.endpoint import write_endpoint

_NAME = 'default'


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


@pytest.fixture(autouse=True)
def configure_logging_mock() -> Generator[mock.MagicMock, None, None]:
    # start_endpoint() configures the logging of the process so it is
    # mocked to not change the logging of the tests.
    with mock.patch(
        'proxystore.endpoint.process.configure_logging',
    ) as configure:
        yield configure


@pytest.fixture
def endpoint_dir(tmp_path: pathlib.Path) -> EndpointDir:
    return EndpointDir.create(_NAME, str(tmp_path), port=1234)


def _write_connection(
    endpoint_dir: EndpointDir,
    hostname: str,
    pid: int = 42,
) -> None:
    endpoint_dir.write_connection(
        ConnectionInfo(
            host='10.0.0.1',
            port=1234,
            token=EndpointToken.generate(),
            tls_fingerprint=None,
            hostname=hostname,
            pid=pid,
        ),
    )


@contextlib.contextmanager
def _lock_holder(
    endpoint_dir: EndpointDir,
) -> Generator[subprocess.Popen[bytes], None, None]:
    """Run a process which holds the lock of the endpoint like an endpoint."""
    code = (
        'import sys, time; '
        'from proxystore.endpoint.directory import EndpointDir; '
        f'EndpointDir({endpoint_dir.path!r}).lock().acquire(); '
        'sys.stdout.write("locked\\n"); sys.stdout.flush(); '
        'time.sleep(1000)'
    )
    process = subprocess.Popen(
        [sys.executable, '-c', code],
        stdout=subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == b'locked\n'
        yield process
    finally:
        process.kill()
        process.wait()


@pytest.mark.usefixtures('_patch_hostname')
@pytest.mark.parametrize('host', ('fqdn', 'ip', 'localhost'))
def test_start_endpoint(
    host: str,
    tmp_path: pathlib.Path,
    configure_logging_mock: mock.MagicMock,
) -> None:
    endpoint_dir = EndpointDir.create(_NAME, str(tmp_path), host=host)
    with open(endpoint_dir.config_path, 'rb') as f:
        before = f.read()

    with mock.patch('proxystore.endpoint.process.serve') as serve:
        start_endpoint(endpoint_dir, log_level='DEBUG')
    serve.assert_called_once_with(endpoint_dir)
    configure_logging_mock.assert_called_once_with(
        'DEBUG',
        endpoint_dir.log_path,
    )
    # Starting the endpoint never modifies the configuration
    with open(endpoint_dir.config_path, 'rb') as f:
        assert f.read() == before


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_detached(endpoint_dir: EndpointDir, caplog) -> None:
    caplog.set_level(logging.INFO)
    with (
        mock.patch('proxystore.endpoint.process.serve', autospec=True),
        mock.patch('daemon.DaemonContext', autospec=True) as context,
    ):
        start_endpoint(endpoint_dir, detach=True)
    context.assert_called_once()
    assert any('daemon' in record.message for record in caplog.records)


def test_start_endpoint_running(endpoint_dir: EndpointDir) -> None:
    with (
        mock.patch.object(
            EndpointDir,
            'status',
            return_value=EndpointStatus.RUNNING,
        ),
        pytest.raises(EndpointRunningError, match='already running'),
    ):
        start_endpoint(endpoint_dir)


def test_start_endpoint_does_not_exist(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.from_name('missing', str(tmp_path))
    with pytest.raises(EndpointNotFoundError, match='does not exist'):
        start_endpoint(endpoint_dir)


def test_start_endpoint_bad_config(endpoint_dir: EndpointDir) -> None:
    with open(endpoint_dir.config_path, 'w') as f:
        f.write('not toml')
    with pytest.raises(EndpointConfigError, match='Unable to parse'):
        start_endpoint(endpoint_dir)


def test_start_endpoint_unresolvable_host(endpoint_dir: EndpointDir) -> None:
    with (
        mock.patch(
            'proxystore.endpoint.process.resolve_host',
            side_effect=OSError('unknown host'),
        ),
        pytest.raises(
            EndpointConfigError,
            match=r'Unable to resolve the host address \(ip\): unknown host',
        ),
    ):
        start_endpoint(endpoint_dir)


@pytest.mark.usefixtures('_patch_hostname')
def test_start_endpoint_stale_connection_file(
    endpoint_dir: EndpointDir,
) -> None:
    # A crashed endpoint leaves its connection file behind
    _write_connection(endpoint_dir, utils.hostname())

    def _serve(*args, **kwargs) -> None:
        assert not os.path.exists(endpoint_dir.connection_path)

    with mock.patch('proxystore.endpoint.process.serve', side_effect=_serve):
        start_endpoint(endpoint_dir)


@pytest.mark.usefixtures('_patch_hostname')
@pytest.mark.parametrize('action', ('start', 'stop'))
def test_endpoint_running_elsewhere(
    action: str,
    endpoint_dir: EndpointDir,
) -> None:
    _write_connection(endpoint_dir, 'other-machine')
    func = start_endpoint if action == 'start' else stop_endpoint
    with pytest.raises(
        EndpointRunningError,
        match=r'running on other-machine \(PID 42\)',
    ):
        func(endpoint_dir)
    # The connection file is not removed
    assert os.path.exists(endpoint_dir.connection_path)


@pytest.mark.timeout(10)
def test_stop_endpoint(endpoint_dir: EndpointDir) -> None:
    with _lock_holder(endpoint_dir) as process:
        _write_connection(endpoint_dir, utils.hostname(), pid=process.pid)
        assert stop_endpoint(endpoint_dir)
        # The process was terminated and reaped by stop_endpoint()
        assert not is_own_process(process.pid)
    assert not os.path.exists(endpoint_dir.connection_path)
    assert endpoint_dir.status() == EndpointStatus.STOPPED


def test_stop_endpoint_not_running(endpoint_dir: EndpointDir) -> None:
    assert not stop_endpoint(endpoint_dir)


def test_stop_endpoint_stale_connection_file(
    endpoint_dir: EndpointDir,
) -> None:
    _write_connection(endpoint_dir, utils.hostname())
    assert not stop_endpoint(endpoint_dir)
    assert not os.path.exists(endpoint_dir.connection_path)


def test_stop_endpoint_starting(endpoint_dir: EndpointDir) -> None:
    lock = endpoint_dir.lock()
    lock.acquire()
    try:
        with pytest.raises(EndpointRunningError, match='still starting'):
            stop_endpoint(endpoint_dir)
    finally:
        lock.release()


def test_stop_endpoint_does_not_exist(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.from_name('missing', str(tmp_path))
    with pytest.raises(EndpointNotFoundError):
        stop_endpoint(endpoint_dir)


def test_wait_for_exit() -> None:
    with (
        mock.patch(
            'proxystore.endpoint.process.is_own_process',
            side_effect=[True, False],
        ),
        mock.patch('time.sleep') as mock_sleep,
    ):
        assert _wait_for_exit(os.getpid(), timeout=1)
    mock_sleep.assert_called_once()

    with mock.patch(
        'proxystore.endpoint.process.is_own_process',
        return_value=True,
    ):
        assert not _wait_for_exit(os.getpid(), timeout=0)


@pytest.mark.timeout(10)
def test_serve(use_uvloop: bool, tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = write_endpoint(
        str(tmp_path),
        'my-endpoint',
        host='127.0.0.1',
    )

    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=serve,
        args=(endpoint_dir,),
        kwargs={'use_uvloop': use_uvloop},
    )
    process.start()

    try:
        wait_for_endpoint(endpoint_dir)
        with EndpointClient.from_dir(endpoint_dir) as client:
            client.set('key', b'value')
            assert client.get('key') == b'value'

        # SIGTERM should cleanly shutdown the endpoint
        process.terminate()
        process.join(timeout=5)
        assert process.exitcode == 0
        assert not os.path.exists(endpoint_dir.connection_path)
    finally:
        terminate_process(process)


def test_serve_missing_config(
    use_uvloop: bool,
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        serve(EndpointDir(str(tmp_path)), use_uvloop=use_uvloop)


def test_serve_does_not_configure_logging(
    use_uvloop: bool,
    tmp_path: pathlib.Path,
) -> None:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    with mock.patch(
        'proxystore.endpoint.process._serve_async',
        mock.AsyncMock(),
    ):
        serve(EndpointDir(str(tmp_path)), use_uvloop=use_uvloop)
    assert root.handlers == handlers
    assert root.level == level


@pytest.fixture
def _restore_root_logger() -> Generator[None, None, None]:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    formatters = [handler.formatter for handler in handlers]
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers = handlers
    for handler, formatter in zip(handlers, formatters, strict=True):
        handler.setFormatter(formatter)
    root.setLevel(level)


@pytest.mark.usefixtures('_restore_root_logger')
def test_configure_logging(tmp_path: pathlib.Path) -> None:
    # The parent directory of the log file is created if necessary
    log_file = os.path.join(tmp_path, 'log-dir', 'log.txt')
    configure_logging('DEBUG', log_file)
    assert logging.getLogger().level == logging.DEBUG
    logging.getLogger('test').debug('message')
    with open(log_file) as f:
        assert '(test) :: message' in f.read()

    # A log file in an existing directory
    log_file = os.path.join(tmp_path, 'log-dir', 'log2.txt')
    configure_logging('INFO', log_file)
    assert os.path.exists(log_file)


@pytest.mark.timeout(15)
def test_serve_then_stop_endpoint(tmp_path: pathlib.Path) -> None:
    endpoint_dir, _ = write_endpoint(
        str(tmp_path),
        'my-endpoint',
        host='127.0.0.1',
    )
    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=serve,
        args=(endpoint_dir,),
        kwargs={'use_uvloop': False},
    )
    process.start()
    try:
        wait_for_endpoint(endpoint_dir)
        assert endpoint_dir.status() == EndpointStatus.RUNNING
        with pytest.raises(EndpointRunningError, match='already running'):
            start_endpoint(endpoint_dir)
        # Stop the endpoint from another process, like the CLI, because
        # stop_endpoint() reaps the endpoint if it is a child process.
        code = (
            'from proxystore.endpoint.directory import EndpointDir; '
            'from proxystore.endpoint.process import stop_endpoint; '
            f'assert stop_endpoint(EndpointDir({endpoint_dir.path!r}))'
        )
        # The endpoint is reaped as soon as it exits or it would be a zombie
        # which stop_endpoint() waits on until its timeout.
        reaper = threading.Thread(target=process.join)
        reaper.start()
        subprocess.run([sys.executable, '-c', code], check=True, timeout=10)
        reaper.join(timeout=5)
        assert process.exitcode == 0
        assert endpoint_dir.status() == EndpointStatus.STOPPED
    finally:
        terminate_process(process)

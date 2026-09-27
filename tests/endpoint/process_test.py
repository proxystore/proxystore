from __future__ import annotations

import logging
import multiprocessing
import os
import pathlib
import time
from collections.abc import Generator
from unittest import mock

import pytest

from proxystore import utils
from proxystore.endpoint.auth import ConnectionInfo
from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.directory import EndpointStatus
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.process import _wait_for_exit
from proxystore.endpoint.process import start_endpoint
from proxystore.endpoint.process import stop_endpoint

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


@pytest.fixture
def endpoint_dir(tmp_path: pathlib.Path) -> EndpointDir:
    return EndpointDir.create(_NAME, str(tmp_path), port=1234)


def _write_connection(endpoint_dir: EndpointDir, hostname: str) -> None:
    endpoint_dir.write_connection(
        ConnectionInfo(
            host='10.0.0.1',
            port=1234,
            token=EndpointToken.generate(),
            tls_fingerprint=None,
            hostname=hostname,
            pid=42,
        ),
    )


def _write_pid(endpoint_dir: EndpointDir, pid: int) -> None:
    with open(endpoint_dir.pid_path, 'w') as f:
        f.write(str(pid))


@pytest.mark.usefixtures('_patch_hostname')
@pytest.mark.parametrize('host', ('fqdn', 'ip', 'localhost'))
def test_start_endpoint(host: str, tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create(_NAME, str(tmp_path), host=host)
    with open(endpoint_dir.config_path, 'rb') as f:
        before = f.read()

    def _serve(*args, **kwargs) -> None:
        # The PID file exists while the endpoint is served
        assert endpoint_dir.running_pid() == os.getpid()

    with mock.patch(
        'proxystore.endpoint.process.serve',
        side_effect=_serve,
    ) as serve:
        start_endpoint(endpoint_dir)
    serve.assert_called_once()
    assert not os.path.exists(endpoint_dir.pid_path)
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
def test_start_endpoint_old_pid_file(endpoint_dir: EndpointDir) -> None:
    # A crashed endpoint leaves its PID and connection files behind
    _write_pid(endpoint_dir, 1)
    _write_connection(endpoint_dir, utils.hostname())
    with (
        mock.patch(
            'proxystore.endpoint.directory.is_own_process',
            return_value=False,
        ),
        mock.patch('proxystore.endpoint.process.serve', autospec=True),
    ):
        start_endpoint(endpoint_dir)
    assert not os.path.exists(endpoint_dir.pid_path)


@pytest.mark.usefixtures('_patch_hostname')
@pytest.mark.parametrize('action', ('start', 'stop'))
def test_endpoint_running_elsewhere(
    action: str,
    endpoint_dir: EndpointDir,
) -> None:
    _write_pid(endpoint_dir, 1)
    _write_connection(endpoint_dir, 'other-machine')
    func = start_endpoint if action == 'start' else stop_endpoint
    with (
        mock.patch(
            'proxystore.endpoint.directory.is_own_process',
            return_value=False,
        ),
        pytest.raises(
            EndpointRunningError,
            match=r'running on other-machine \(PID 42\)',
        ),
    ):
        func(endpoint_dir)
    # The PID file is not removed
    assert os.path.exists(endpoint_dir.pid_path)


@pytest.mark.timeout(5)
def test_stop_endpoint(endpoint_dir: EndpointDir) -> None:
    # Create a fake process to kill
    context = multiprocessing.get_context('spawn')
    process = context.Process(target=time.sleep, args=(1000,))
    process.start()
    assert process.pid is not None
    _write_pid(endpoint_dir, process.pid)

    assert stop_endpoint(endpoint_dir)
    assert not os.path.exists(endpoint_dir.pid_path)
    # Process was terminated so this should happen immediately
    process.join()


def test_stop_endpoint_not_running(endpoint_dir: EndpointDir) -> None:
    assert not stop_endpoint(endpoint_dir)


def test_stop_endpoint_dangling_pid_file(endpoint_dir: EndpointDir) -> None:
    _write_pid(endpoint_dir, 1)
    with mock.patch(
        'proxystore.endpoint.directory.is_own_process',
        return_value=False,
    ):
        assert not stop_endpoint(endpoint_dir)
    assert not os.path.exists(endpoint_dir.pid_path)


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

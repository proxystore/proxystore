from __future__ import annotations

import os
import pathlib
import stat

import pytest

from proxystore.endpoint.auth import ConnectionInfo
from proxystore.endpoint.auth import generate_token
from proxystore.endpoint.directory import EndpointDir


def test_endpoint_dir_paths() -> None:
    endpoint_dir = EndpointDir('/path/to/endpoint')
    assert os.fspath(endpoint_dir) == str(endpoint_dir) == '/path/to/endpoint'

    paths = [
        endpoint_dir.config_path,
        endpoint_dir.database_path,
        endpoint_dir.log_path,
        endpoint_dir.pid_path,
        endpoint_dir.connection_path,
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


@pytest.mark.parametrize('fingerprint', ('abcd', None))
def test_connection_lifecycle(
    fingerprint: str | None,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        endpoint_dir.read_connection()

    info = ConnectionInfo('localhost', 1234, generate_token(), fingerprint)
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
    ours = ConnectionInfo('localhost', 1234, generate_token(), None)
    theirs = ConnectionInfo('localhost', 1234, generate_token(), None)

    # No file to remove
    endpoint_dir.remove_connection(ours)

    endpoint_dir.write_connection(theirs)
    endpoint_dir.remove_connection(ours)
    assert endpoint_dir.read_connection() == theirs

    endpoint_dir.remove_connection(theirs)
    assert not os.path.exists(endpoint_dir.connection_path)


@pytest.mark.parametrize(
    'contents',
    (
        'not json',
        '[]',
        '{}',
        '{"host": "h", "port": 1, "token": "zz", "tls_fingerprint": null}',
        '{"host": "h", "port": 1, "token": "abcd", "tls_fingerprint": null}',
        '{"host": 1, "port": 1, "token": "%s", "tls_fingerprint": null}',
        '{"host": "h", "port": "1", "token": "%s", "tls_fingerprint": null}',
        '{"host": "h", "port": 1, "token": "%s", "tls_fingerprint": 1}',
    ),
)
def test_read_connection_malformed(
    contents: str,
    tmp_path: pathlib.Path,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    if '%s' in contents:
        contents = contents % generate_token().hex()
    with open(endpoint_dir.connection_path, 'w') as f:
        f.write(contents)
    with pytest.raises(ValueError, match='malformed'):
        endpoint_dir.read_connection()

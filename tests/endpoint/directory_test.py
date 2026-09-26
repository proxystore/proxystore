from __future__ import annotations

import os
import pathlib
import stat

import pytest

from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.directory import EndpointDir


def test_endpoint_dir_paths() -> None:
    endpoint_dir = EndpointDir('/path/to/endpoint')
    assert os.fspath(endpoint_dir) == str(endpoint_dir) == '/path/to/endpoint'

    paths = [
        endpoint_dir.config_path,
        endpoint_dir.database_path,
        endpoint_dir.log_path,
        endpoint_dir.pid_path,
        endpoint_dir.token_path,
        endpoint_dir.tls_cert_path,
        endpoint_dir.tls_key_path,
    ]
    assert all(os.path.dirname(p) == '/path/to/endpoint' for p in paths)
    assert len(set(paths)) == len(paths)


@pytest.mark.parametrize(
    ('mode', 'expected'),
    ((0o700, 0o700), (0o755, 0o755), (0o775, 0o755), (0o777, 0o755)),
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


@pytest.mark.parametrize('tls', (True, False))
def test_credentials_lifecycle(tls: bool, tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        endpoint_dir.load_credentials(tls=tls)

    credentials = endpoint_dir.create_credentials(tls=tls, common_name='x')
    assert len(credentials.token) == TOKEN_SIZE
    assert (credentials.tls_fingerprint is not None) == tls
    assert endpoint_dir.load_credentials(tls=tls) == credentials
    if tls:
        endpoint_dir.server_ssl_context()

    endpoint_dir.remove_credentials()
    assert os.listdir(tmp_path) == []
    # Removing again is a no-op
    endpoint_dir.remove_credentials()

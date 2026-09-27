from __future__ import annotations

import os
import pathlib
import stat
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import ConnectionInfo
from proxystore.endpoint.files import VersionedFile
from proxystore.endpoint.files import write_private_file
from proxystore.endpoint.p2p.addrs import PeerAddrCacheFile
from proxystore.endpoint.peers import PeersConfig


def _mode(path: pathlib.Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def test_write_private_file_mode(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'file'
    old_umask = os.umask(0)
    try:
        write_private_file(str(path), b'secret')
    finally:
        os.umask(old_umask)
    assert _mode(path) == 0o600
    assert path.read_bytes() == b'secret'


def test_write_private_file_resets_existing_mode(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / 'file'
    path.write_bytes(b'old contents that are longer')
    os.chmod(path, 0o644)

    write_private_file(str(path), b'new')

    assert _mode(path) == 0o600
    assert path.read_bytes() == b'new'


def test_write_private_file_is_atomic(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'file'
    path.write_bytes(b'old')
    with (
        mock.patch('os.replace', side_effect=OSError('failed')),
        pytest.raises(OSError, match='failed'),
    ):
        write_private_file(str(path), b'new')

    # The original file is untouched and the temporary file is removed
    assert path.read_bytes() == b'old'
    assert os.listdir(tmp_path) == ['file']


@pytest.mark.parametrize(
    'model',
    (EndpointConfig, PeersConfig, ConnectionInfo, PeerAddrCacheFile),
)
@pytest.mark.parametrize('version', (0, 2))
def test_versioned_file_unsupported_version(
    model: type[VersionedFile],
    version: Any,
) -> None:
    assert model.model_fields['version'].default == 1
    with pytest.raises(ValueError, match='only supports version 1'):
        model.model_validate({'version': version})

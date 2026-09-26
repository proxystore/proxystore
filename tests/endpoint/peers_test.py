from __future__ import annotations

import os
import pathlib
from typing import Any

import pytest

from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.peers import Allowlist
from proxystore.endpoint.peers import PeersConfig
from proxystore.endpoint.peers import read_peers

_ID1 = EndpointId.random()
_ID2 = EndpointId.random()


def test_peers_config() -> None:
    peers = PeersConfig(peers={'a': _ID1, 'b': _ID2.upper()})
    assert peers.peers == {'a': _ID1, 'b': _ID2}
    assert peers.name_of(_ID1) == 'a'
    assert peers.name_of(EndpointId.random()) is None
    assert PeersConfig().peers == {}


@pytest.mark.parametrize(
    ('peers', 'error'),
    (
        ([], 'table of names'),
        ({'bad name': _ID1}, 'alphanumeric'),
        ({'a': 'not-an-id'}, 'not a valid endpoint ID'),
        ({'a': _ID1, 'b': _ID1}, 'same endpoint ID'),
    ),
)
def test_peers_config_invalid(peers: Any, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        PeersConfig(peers=peers)


def test_read_write_peers(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    assert endpoint_dir.read_peers() == PeersConfig()

    peers = PeersConfig(peers={'a': _ID1, 'b': _ID2})
    endpoint_dir.write_peers(peers)
    assert endpoint_dir.read_peers() == peers
    assert oct(os.stat(endpoint_dir.peers_path).st_mode & 0o777) == '0o600'


def test_read_peers_malformed(tmp_path: pathlib.Path) -> None:
    path = str(tmp_path / 'peers.toml')
    with open(path, 'w') as f:
        f.write('[peers]\na = "not-an-id"\n')
    with pytest.raises(ValueError, match='Unable to parse'):
        read_peers(path)

    with open(path, 'w') as f:
        f.write('not toml')
    with pytest.raises(ValueError, match='Unable to parse'):
        read_peers(path)


def test_allowlist_reload(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    allowlist = Allowlist(endpoint_dir.peers_path)

    # Missing file is an empty allowlist
    assert allowlist.reload() == set()
    assert not allowlist.allowed(_ID1)

    endpoint_dir.write_peers(PeersConfig(peers={'a': _ID1, 'b': _ID2}))
    assert allowlist.reload() == set()
    assert allowlist.allowed(_ID1)
    assert allowlist.allowed(_ID2)
    assert allowlist.name_of(_ID2) == 'b'
    # File is unchanged so reloading is a no-op
    assert allowlist.reload() == set()

    endpoint_dir.write_peers(PeersConfig(peers={'a': _ID1}))
    assert allowlist.reload() == {_ID2}
    assert allowlist.allowed(_ID1)
    assert not allowlist.allowed(_ID2)
    assert allowlist.name_of(_ID2) is None

    os.remove(endpoint_dir.peers_path)
    assert allowlist.reload() == {_ID1}
    assert allowlist.peers == PeersConfig()


def test_allowlist_malformed_denies_all(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    endpoint_dir = EndpointDir(str(tmp_path))
    endpoint_dir.write_peers(PeersConfig(peers={'a': _ID1}))
    allowlist = Allowlist(endpoint_dir.peers_path)
    assert allowlist.allowed(_ID1)

    with open(endpoint_dir.peers_path, 'w') as f:
        f.write('[peers]\na = "not-an-id"\nb = "also-not-an-id"\n')
    assert allowlist.reload() == {_ID1}
    assert not allowlist.allowed(_ID1)
    assert any('All peers will be denied' in r.message for r in caplog.records)

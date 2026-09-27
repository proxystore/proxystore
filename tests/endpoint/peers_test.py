from __future__ import annotations

import os
import pathlib
import time
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.peers import PeerExistsError
from proxystore.endpoint.peers import Peers
from proxystore.endpoint.peers import PeersConfig

_ID1 = EndpointId.random()
_ID2 = EndpointId.random()
_OWNER = EndpointId.random()


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
    peers = Peers(str(tmp_path / 'peers.toml'))
    assert peers.read() == PeersConfig()

    config = PeersConfig(peers={'a': _ID1, 'b': _ID2})
    peers.write(config)
    assert peers.read() == config
    assert oct(os.stat(peers.path).st_mode & 0o777) == '0o600'


def test_read_peers_malformed(tmp_path: pathlib.Path) -> None:
    peers = Peers(str(tmp_path / 'peers.toml'))
    with open(peers.path, 'w') as f:
        f.write('[peers]\na = "not-an-id"\n')
    with pytest.raises(ValueError, match='malformed'):
        peers.read()

    with open(peers.path, 'w') as f:
        f.write('not toml')
    with pytest.raises(ValueError, match='malformed'):
        peers.read()


def test_add_remove_peers(tmp_path: pathlib.Path) -> None:
    peers = Peers(str(tmp_path / 'peers.toml'))
    assert peers.add('a', _ID1.upper()) == _ID1
    assert peers.add('b', _ID2) == _ID2
    assert peers.read().peers == {'a': _ID1, 'b': _ID2}

    assert peers.remove('a') == _ID1
    assert peers.read().peers == {'b': _ID2}
    with pytest.raises(ValueError, match='No peer named a'):
        peers.remove('a')


@pytest.mark.parametrize(
    ('name', 'endpoint_id', 'error', 'match'),
    (
        ('bad name', str(_ID2), ValueError, 'alphanumeric'),
        ('c', 'xyz', ValueError, 'not a valid endpoint ID'),
        ('c', '02' * 32, ValueError, 'not a valid public key'),
        ('c', str(_OWNER), ValueError, 'peer of itself'),
        ('a', str(_ID2), PeerExistsError, 'peer named a already exists'),
        ('c', str(_ID1), ValueError, 'already a peer named a'),
    ),
)
def test_add_peer_errors(
    name: str,
    endpoint_id: str,
    error: type[Exception],
    match: str,
    tmp_path: pathlib.Path,
) -> None:
    peers = Peers(str(tmp_path / 'peers.toml'), owner_id=_OWNER)
    peers.add('a', _ID1)
    with pytest.raises(error, match=match):
        peers.add(name, endpoint_id)
    assert peers.read().peers == {'a': _ID1}


def test_endpoint_dir_peers(tmp_path: pathlib.Path) -> None:
    endpoint_dir = EndpointDir.create('ep', str(tmp_path), port=1234)
    peers = endpoint_dir.peers()
    assert peers.path == endpoint_dir.peers_path
    assert peers.owner_id == endpoint_dir.read_config().id
    assert peers.allowlist().path == endpoint_dir.peers_path


def test_allowlist_reload(tmp_path: pathlib.Path) -> None:
    peers = Peers(str(tmp_path / 'peers.toml'))
    allowlist = peers.allowlist(reload_interval=0)

    # Missing file is an empty allowlist
    assert not allowlist.allowed(_ID1)

    peers.write(PeersConfig(peers={'a': _ID1, 'b': _ID2}))
    assert allowlist.allowed(_ID1)
    assert allowlist.allowed(_ID2)
    assert allowlist.name_of(_ID2) == 'b'
    # File is unchanged so reloading is a no-op
    with mock.patch.object(Peers, 'read') as read:
        allowlist.reload()
    read.assert_not_called()

    peers.write(PeersConfig(peers={'a': _ID1}))
    assert allowlist.allowed(_ID1)
    assert not allowlist.allowed(_ID2)
    assert allowlist.name_of(_ID2) is None

    os.remove(peers.path)
    assert allowlist.peers == PeersConfig()


def test_allowlist_malformed_denies_all(
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    peers = Peers(str(tmp_path / 'peers.toml'))
    peers.write(PeersConfig(peers={'a': _ID1}))
    allowlist = peers.allowlist(reload_interval=0)
    assert allowlist.allowed(_ID1)

    with open(peers.path, 'w') as f:
        f.write('[peers]\na = "not-an-id"\nb = "also-not-an-id"\n')
    assert not allowlist.allowed(_ID1)
    assert any('All peers will be denied' in r.message for r in caplog.records)


def test_peers_version() -> None:
    assert PeersConfig().version == 1
    with pytest.raises(ValueError, match='only supports version 1'):
        PeersConfig(version=2)


def test_allowlist_reload_interval(tmp_path: pathlib.Path) -> None:
    peers = Peers(str(tmp_path / 'peers.toml'))
    peers.write(PeersConfig(peers={'a': _ID1}))
    allowlist = peers.allowlist(reload_interval=60)
    assert allowlist.reload_interval == 60
    assert allowlist.allowed(_ID1)

    # Changes are not checked until the interval passes
    os.remove(peers.path)
    with mock.patch('os.stat', wraps=os.stat) as stat:
        assert allowlist.allowed(_ID1)
    stat.assert_not_called()

    allowlist.reload(force=True)
    assert not allowlist.allowed(_ID1)

    with mock.patch('time.monotonic', return_value=time.monotonic() + 61):
        peers.write(PeersConfig(peers={'a': _ID1}))
        assert allowlist.allowed(_ID1)

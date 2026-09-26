from __future__ import annotations

import json
import os
import pathlib
import stat

import iroh

from proxystore.endpoint.identity import EndpointId
from proxystore.p2p.addrs import load_peer_addrs
from proxystore.p2p.addrs import save_peer_addrs


def test_load_missing(tmp_path: pathlib.Path) -> None:
    assert load_peer_addrs(str(tmp_path / 'peer-addrs.json')) == {}


def test_save_load_round_trip(tmp_path: pathlib.Path) -> None:
    path = str(tmp_path / 'peer-addrs.json')
    id1, id2 = EndpointId.random(), EndpointId.random()
    addrs = {
        id1: iroh.EndpointAddr(
            iroh.EndpointId.from_string(id1),
            'https://relay.example.com',
            ['127.0.0.1:1234'],
        ),
        id2: iroh.EndpointAddr(iroh.EndpointId.from_string(id2), None, []),
    }
    save_peer_addrs(path, addrs)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600

    loaded = load_peer_addrs(path)
    assert set(loaded) == {id1, id2}
    assert loaded[id1].relay_url() == 'https://relay.example.com'
    assert loaded[id1].direct_addresses() == ['127.0.0.1:1234']
    assert str(loaded[id1].id()) == id1
    assert loaded[id2].relay_url() is None
    assert loaded[id2].direct_addresses() == []


def test_load_malformed_file(tmp_path: pathlib.Path, caplog) -> None:
    path = tmp_path / 'peer-addrs.json'
    path.write_text('not json')
    assert load_peer_addrs(str(path)) == {}

    path.write_text('[]')
    assert load_peer_addrs(str(path)) == {}
    assert len(caplog.records) == 2


def test_load_malformed_entries(tmp_path: pathlib.Path, caplog) -> None:
    good = EndpointId.random()
    data = {
        good: {'relay_url': None, 'addresses': ['127.0.0.1:1']},
        'not-an-id': {'relay_url': None, 'addresses': []},
        EndpointId.random(): 'not a dict',
        EndpointId.random(): {'relay_url': 42},
        EndpointId.random(): {'addresses': 'not a list'},
        EndpointId.random(): {'addresses': [42]},
    }
    path = tmp_path / 'peer-addrs.json'
    path.write_text(json.dumps(data))

    addrs = load_peer_addrs(str(path))
    assert list(addrs) == [good]
    assert len(caplog.records) == len(data) - 1

from __future__ import annotations

import json
import os
import pathlib
import stat
from typing import Any

import iroh
import pytest

from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.p2p.addrs import PeerAddrCache


def test_load_missing(tmp_path: pathlib.Path) -> None:
    assert PeerAddrCache(str(tmp_path / 'peer-addrs.json')).load() == {}


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
    PeerAddrCache(path).save(addrs)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600

    loaded = PeerAddrCache(path).load()
    assert set(loaded) == {id1, id2}
    assert loaded[id1].relay_url() == 'https://relay.example.com'
    assert loaded[id1].direct_addresses() == ['127.0.0.1:1234']
    assert str(loaded[id1].id()) == id1
    assert loaded[id2].relay_url() is None
    assert loaded[id2].direct_addresses() == []


def test_load_malformed_file(tmp_path: pathlib.Path, caplog) -> None:
    path = tmp_path / 'peer-addrs.json'
    path.write_text('not json')
    assert PeerAddrCache(str(path)).load() == {}

    path.write_text('[]')
    assert PeerAddrCache(str(path)).load() == {}

    path.write_text(json.dumps({'version': 1, 'peers': []}))
    assert PeerAddrCache(str(path)).load() == {}
    assert len(caplog.records) == 3


@pytest.mark.parametrize(
    ('version', 'match'),
    ((None, 'malformed'), (2, 'only supports version 1')),
)
def test_load_unsupported_version(
    version: int | None,
    match: str,
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    path = tmp_path / 'peer-addrs.json'
    path.write_text(json.dumps({'version': version, 'peers': {}}))
    assert PeerAddrCache(str(path)).load() == {}
    assert any(match in r.message for r in caplog.records)


@pytest.mark.parametrize(
    'entry',
    (
        {'not-an-id': {'relay_url': None, 'addresses': []}},
        {EndpointId.random(): 'not a dict'},
        {EndpointId.random(): {'relay_url': 42}},
        {EndpointId.random(): {'addresses': 'not a list'}},
        {EndpointId.random(): {'addresses': [42]}},
        {EndpointId.random(): {'unknown': 42}},
    ),
)
def test_load_malformed_entries(
    entry: dict[str, Any],
    tmp_path: pathlib.Path,
    caplog,
) -> None:
    good: dict[str, Any] = {
        EndpointId.random(): {'addresses': ['127.0.0.1:1']}
    }
    path = tmp_path / 'peer-addrs.json'
    data = {'version': 1, 'peers': {**good, **entry}}
    path.write_text(json.dumps(data))
    # The cache is ignored if any entry is malformed
    assert PeerAddrCache(str(path)).load() == {}
    assert len(caplog.records) == 1
    assert 'malformed' in caplog.records[0].message

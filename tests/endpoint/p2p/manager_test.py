from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import pathlib
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import iroh
import pytest

from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.exceptions import PeerUnavailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.p2p.addrs import PeerAddrCache
from proxystore.endpoint.p2p.manager import CloseCode
from proxystore.endpoint.p2p.manager import PathInfo
from proxystore.endpoint.p2p.manager import PeerManager
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import Status
from testing.p2p import allow_peer
from testing.p2p import allowlist
from testing.p2p import connect_peers
from testing.p2p import local_peer_manager
from testing.p2p import LOCAL_PEER_OPTIONS

_MANAGER = 'proxystore.endpoint.p2p.manager'


class _IrohError(iroh.IrohError):
    # IrohError has no default constructor.
    def __init__(self) -> None:
        pass

    def message(self) -> str:
        return 'boom'


Handled = list[tuple[EndpointId, int, dict[str, Any], bytes | bytearray]]


def _echo_handler(handled: Handled) -> Any:
    async def _handler(peer: EndpointId, request: Message) -> Message:
        handled.append((peer, request.code, request.meta, request.data))
        if request.meta.get('raise'):
            raise RuntimeError('handler failed')
        return Message(Status.OK, {'echo': request.meta}, request.data)

    return _handler


async def _request(
    manager: PeerManager,
    peer_id: EndpointId,
    code: int,
    meta: dict[str, Any] | None = None,
    data: Any = b'',
) -> tuple[int, dict[str, Any], bytes | bytearray]:
    response = await manager.request(
        peer_id,
        Message(code, {} if meta is None else meta, data),
    )
    return response.code, response.meta, response.data


@pytest.fixture
async def managers(
    tmp_path: pathlib.Path,
) -> AsyncGenerator[tuple[PeerManager, PeerManager, Handled], None]:
    handled: Handled = []
    manager1 = local_peer_manager(str(tmp_path / 'm1'))
    manager2 = local_peer_manager(
        str(tmp_path / 'm2'),
        max_request_size=1000,
    )
    await manager1.start(_echo_handler(handled))
    await manager2.start(_echo_handler(handled))
    connect_peers(manager1, manager2)
    yield manager1, manager2, handled
    await manager1.close()
    await manager2.close()


async def test_request(managers) -> None:
    manager1, manager2, handled = managers

    status, meta, data = await _request(
        manager1,
        manager2.id,
        Op.GET,
        {'key': 'abc'},
        b'data',
    )
    assert status == Status.OK
    assert meta == {'echo': {'key': 'abc'}}
    assert data == b'data'
    assert handled == [(manager1.id, Op.GET, {'key': 'abc'}, b'data')]

    # No metadata or data
    status, meta, data = await _request(manager2, manager1.id, Op.EXISTS)
    assert status == Status.OK
    assert meta == {'echo': {}}
    assert data == b''


async def test_concurrent_requests(managers) -> None:
    manager1, manager2, _ = managers
    results = await asyncio.gather(
        *(
            _request(manager1, manager2.id, Op.GET, {'i': i}, bytes([i]))
            for i in range(20)
        ),
    )
    assert [data for _, _, data in results] == [bytes([i]) for i in range(20)]
    # Requests share a single connection
    assert len(manager1._outgoing) == 1


async def test_large_request(managers, tmp_path: pathlib.Path) -> None:
    manager1, _, _ = managers
    manager3 = local_peer_manager(str(tmp_path / 'm3'))
    await manager3.start(_echo_handler([]))
    try:
        connect_peers(manager1, manager3)
        data = os.urandom(1000)
        # Use a small chunk size to test data spanning multiple chunks
        with mock.patch('proxystore.endpoint.p2p.manager._CHUNK_SIZE', 300):
            _, _, response = await _request(
                manager1,
                manager3.id,
                Op.SET,
                {},
                memoryview(data),
            )
        assert isinstance(response, bytearray)
        assert response == data
    finally:
        await manager3.close()


async def test_request_too_large(managers) -> None:
    manager1, manager2, handled = managers
    status, meta, _ = await _request(
        manager1,
        manager2.id,
        Op.SET,
        {},
        b'x' * 1001,
    )
    assert status == Status.TOO_LARGE
    assert 'exceeds the maximum object size' in meta['error']
    assert len(handled) == 0

    # Connection is still usable
    status, _, _ = await _request(manager1, manager2.id, Op.SET, {}, b'x')
    assert status == Status.OK


async def test_handler_error(managers) -> None:
    manager1, manager2, _ = managers
    status, meta, _ = await _request(
        manager1,
        manager2.id,
        Op.GET,
        {'raise': True},
    )
    assert status == Status.ERROR
    assert 'handler failed' in meta['error']


async def test_bad_request(managers) -> None:
    manager1, manager2, _ = managers
    connection, _ = await manager1._get_connection(manager2.id)
    stream = await connection.open_bi()
    # Header with invalid metadata
    await stream.send().write_all(Header(Op.GET, 0, 0, 1, 0).pack())
    await stream.send().write_all(b'x')
    await stream.send().finish()
    response = await stream.recv().read_to_end(1000)
    assert response[0] == Status.BAD_REQUEST


async def test_request_not_in_allowlist(managers) -> None:
    manager1, manager2, _ = managers
    os.remove(allowlist(manager1).path)
    with pytest.raises(PeerNotAllowedError, match='not in the allowlist'):
        await _request(manager1, manager2.id, Op.GET)


async def test_peer_refuses_connection(managers) -> None:
    manager1, manager2, handled = managers
    os.remove(allowlist(manager2).path)
    with pytest.raises(PeerNotAllowedError, match='refused the connection'):
        await _request(manager1, manager2.id, Op.GET)
    assert len(handled) == 0


async def test_revoke_peer(managers) -> None:
    manager1, manager2, handled = managers
    await _request(manager1, manager2.id, Op.GET)
    assert manager1.id in manager2._incoming

    # Removing the peer closes its connections and denies new requests.
    os.remove(allowlist(manager2).path)
    with pytest.raises(PeerNotAllowedError):
        await _request(manager1, manager2.id, Op.GET)
    assert manager1.id not in manager2._incoming
    assert len(handled) == 1

    # Adding the peer back allows it to reconnect.
    allow_peer(manager2, manager1, 'peer')
    status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK


async def test_revoke_detected_on_request(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    await _request(manager2, manager1.id, Op.GET)
    assert manager2.id in manager1._outgoing

    # Manager 1 closes its connections to the removed peer the next time it
    # checks the allowlist.
    os.remove(allowlist(manager1).path)
    with pytest.raises(PeerNotAllowedError):
        await _request(manager1, manager2.id, Op.GET)
    assert manager2.id not in manager1._outgoing
    assert manager2.id not in manager1._incoming


async def test_stale_connection_retry(managers, tmp_path) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    stale = manager1._outgoing[manager2.id]

    # Simulate the peer restarting by closing the connection from the peer.
    for connection in manager2._incoming[manager1.id]:
        connection.close(CloseCode.SHUTDOWN, b'restart')
    await stale.closed()

    # Mark the stale connection as open so the manager tries to use it.
    with mock.patch.object(
        type(stale),
        'close_reason',
        side_effect=[None, 'closed by peer: restart (code 0)'],
    ):
        status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK
    assert manager1._outgoing[manager2.id] is not stale


async def test_request_fails_on_fresh_connection(managers) -> None:
    manager1, manager2, _ = managers

    async def _fail(*args: Any) -> Any:
        raise _IrohError

    with (
        mock.patch.object(manager1, '_exchange', side_effect=_fail),
        pytest.raises(PeerUnavailableError, match='boom'),
    ):
        await _request(manager1, manager2.id, Op.GET)


async def test_connect_error(tmp_path: pathlib.Path) -> None:
    manager1 = local_peer_manager(str(tmp_path / 'm1'))
    manager2 = local_peer_manager(str(tmp_path / 'm2'))
    await manager1.start(_echo_handler([]))
    try:
        allow_peer(manager1, manager2, 'peer')
        # No address or discovery for the peer
        with pytest.raises(PeerUnavailableError, match='Failed to connect'):
            await _request(manager1, manager2.id, Op.GET)
    finally:
        await manager1.close()


async def test_connect_timeout(tmp_path: pathlib.Path) -> None:
    manager1 = local_peer_manager(
        str(tmp_path / 'm1'),
        options=dataclasses.replace(LOCAL_PEER_OPTIONS, connect_timeout=0.1),
    )
    manager2 = local_peer_manager(str(tmp_path / 'm2'))
    await manager1.start(_echo_handler([]))
    try:
        allow_peer(manager1, manager2, 'peer')

        async def _hang(*args: Any) -> None:
            await asyncio.sleep(10)

        with (
            mock.patch.object(iroh.Endpoint, 'connect', _hang),
            pytest.raises(PeerConnectionTimeoutError),
        ):
            await _request(manager1, manager2.id, Op.GET)
    finally:
        await manager1.close()


async def test_handle_incoming_accept_error(managers) -> None:
    manager1, _, _ = managers
    incoming = mock.AsyncMock()
    incoming.accept.side_effect = _IrohError()
    await manager1._handle_incoming(incoming)


async def test_stream_error_is_logged(managers, caplog) -> None:
    manager1, manager2, _ = managers
    stream = mock.MagicMock()
    stream.recv.return_value.read_exact = mock.AsyncMock(
        side_effect=_IrohError(),
    )
    await manager1._handle_stream(manager2.id, stream)


async def test_not_started(tmp_path: pathlib.Path) -> None:
    manager = local_peer_manager(str(tmp_path))
    with pytest.raises(RuntimeError, match='not been started'):
        manager.addr()
    # Closing a manager that was never started is okay
    await manager.close()


async def test_start_and_close_idempotent(tmp_path: pathlib.Path) -> None:
    manager = local_peer_manager(str(tmp_path))
    await manager.start(_echo_handler([]))
    endpoint = manager.endpoint
    await manager.start(_echo_handler([]))
    assert manager.endpoint is endpoint
    await manager.close()
    await manager.close()


async def test_online(tmp_path: pathlib.Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    manager = local_peer_manager(
        str(tmp_path),
        options=dataclasses.replace(LOCAL_PEER_OPTIONS, online_timeout=1),
    )
    with mock.patch.object(iroh.Endpoint, 'online', mock.AsyncMock()):
        await manager.start(_echo_handler([]))
        assert manager._online_task is not None
        await manager._online_task
    await manager.close()
    assert any('connected to home relay' in r.message for r in caplog.records)


async def test_drop_outgoing_replaced_connection(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    current = manager1._outgoing[manager2.id]
    # Dropping a connection that was already replaced keeps the current one.
    manager1._drop_outgoing(manager2.id, mock.MagicMock())
    assert manager1._outgoing[manager2.id] is current


async def test_exchange_write_error_reads_response(managers) -> None:
    manager1, _, _ = managers
    connection = mock.AsyncMock()
    connection.open_bi.return_value = mock.MagicMock()
    expected = Message(Status.TOO_LARGE, {'error': 'too large'})
    with (
        mock.patch(
            'proxystore.endpoint.p2p.manager._write_message',
            side_effect=_IrohError(),
        ),
        mock.patch(
            'proxystore.endpoint.p2p.manager._read_message',
            return_value=expected,
        ),
    ):
        request = Message(Op.SET, {}, b'x')
        response = await manager1._exchange(connection, request)
    assert response == expected


@pytest.mark.parametrize('write_fails', (True, False))
async def test_exchange_read_error(managers, write_fails: bool) -> None:
    manager1, _, _ = managers
    connection = mock.AsyncMock()
    connection.open_bi.return_value = mock.MagicMock()
    write_error = _IrohError()
    read_error = _IrohError()
    with (
        mock.patch(
            'proxystore.endpoint.p2p.manager._write_message',
            side_effect=write_error if write_fails else None,
        ),
        mock.patch(
            'proxystore.endpoint.p2p.manager._read_message',
            side_effect=read_error,
        ),
        pytest.raises(_IrohError) as exc_info,
    ):
        await manager1._exchange(connection, Message(Op.SET, {}, b'x'))
    assert exc_info.value is (write_error if write_fails else read_error)


async def test_addr_cache(tmp_path: pathlib.Path) -> None:
    cache1 = str(tmp_path / 'm1' / 'peer-addrs.json')
    cache2 = str(tmp_path / 'm2' / 'peer-addrs.json')
    manager1 = local_peer_manager(
        str(tmp_path / 'm1'),
        addr_cache=PeerAddrCache(cache1),
    )
    manager2 = local_peer_manager(
        str(tmp_path / 'm2'),
        addr_cache=PeerAddrCache(cache2),
    )
    await manager1.start(_echo_handler([]))
    await manager2.start(_echo_handler([]))
    try:
        allow_peer(manager1, manager2, 'peer')
        allow_peer(manager2, manager1, 'peer')
        manager1.add_peer_addr(manager2.addr())

        await _request(manager1, manager2.id, Op.GET)
        # Both the dialing and accepting peer save the address of the other
        assert list(PeerAddrCache(cache1).load()) == [manager2.id]
        assert list(PeerAddrCache(cache2).load()) == [manager1.id]

        # Manager 2 can now reach manager 1 without being given its address
        status, _, _ = await _request(manager2, manager1.id, Op.GET)
        assert status == Status.OK
    finally:
        await manager1.close()
        await manager2.close()

    # A new manager with the same key loads the cached addresses
    manager3 = PeerManager(
        manager1._secret_key,
        manager1.policy,
        options=LOCAL_PEER_OPTIONS,
        addr_cache=PeerAddrCache(cache1),
    )
    await manager3.start(_echo_handler([]))
    try:
        assert list(manager3._addr_hints) == [manager2.id]
    finally:
        await manager3.close()


async def test_addr_cache_prunes_removed_peers(managers, tmp_path) -> None:
    manager1, manager2, _ = managers
    cache = str(tmp_path / 'peer-addrs.json')
    manager1._addr_cache = PeerAddrCache(cache)
    removed = local_peer_manager(str(tmp_path / 'removed'))
    await removed.start(_echo_handler([]))
    try:
        manager1.add_peer_addr(removed.addr())
        await _request(manager1, manager2.id, Op.GET)
        assert list(PeerAddrCache(cache).load()) == [manager2.id]
    finally:
        await removed.close()


async def test_addr_cache_save_error(managers, caplog) -> None:
    manager1, manager2, _ = managers
    manager1._addr_cache = PeerAddrCache('/does/not/exist/peer-addrs.json')
    status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK
    assert any('failed to save' in r.message for r in caplog.records)


async def test_stale_addr_falls_back_to_discovery(managers) -> None:
    manager1, manager2, _ = managers
    good = manager1._addr_hints[manager2.id]
    stale = iroh.EndpointAddr(good.id(), None, ['127.0.0.1:1'])
    manager1.add_peer_addr(stale)

    real_dial = manager1._dial
    calls: list[iroh.EndpointAddr] = []

    async def _dial(peer_id: EndpointId, addr: iroh.EndpointAddr) -> Any:
        calls.append(addr)
        if len(calls) == 1:
            raise PeerUnavailableError('stale')
        # Discovery is not available in tests so use the good address.
        return await real_dial(peer_id, good)

    with mock.patch.object(manager1, '_dial', _dial):
        status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK
    assert calls[0] is stale
    assert calls[1].direct_addresses() == []


async def test_stale_addr_timeout_not_retried(managers) -> None:
    manager1, manager2, _ = managers
    with (
        mock.patch.object(
            manager1,
            '_dial',
            side_effect=PeerConnectionTimeoutError('timeout'),
        ) as dial,
        pytest.raises(PeerConnectionTimeoutError),
    ):
        await _request(manager1, manager2.id, Op.GET)
    assert dial.call_count == 1


async def test_online_timeout(tmp_path: pathlib.Path, caplog) -> None:
    # Relays are disabled so the endpoint never connects to a home relay.
    manager = local_peer_manager(
        str(tmp_path),
        options=dataclasses.replace(LOCAL_PEER_OPTIONS, online_timeout=0.01),
    )
    await manager.start(_echo_handler([]))
    assert manager._online_task is not None
    await manager._online_task
    await manager.close()
    assert any('not connected to a home' in r.message for r in caplog.records)


def _path(*, selected: bool, relay: bool, addr: str, rtt: int = 5) -> Any:
    return mock.MagicMock(
        is_selected=selected,
        is_relay=relay,
        remote_addr=addr,
        rtt_ms=rtt,
    )


def test_path_info_from_connection() -> None:
    connection = mock.MagicMock()
    connection.paths.return_value = [
        _path(selected=False, relay=True, addr='https://relay'),
        _path(selected=True, relay=False, addr='1.2.3.4:5', rtt=7),
    ]
    path = PathInfo.from_connection(connection)
    assert path == PathInfo(relayed=False, remote_addr='1.2.3.4:5', rtt_ms=7)
    assert path.describe() == 'direct to 1.2.3.4:5 (rtt 7 ms)'

    connection.paths.return_value = [
        _path(selected=True, relay=True, addr='https://relay'),
    ]
    path = PathInfo.from_connection(connection)
    assert path is not None
    assert path.describe() == 'relayed via https://relay (rtt 5 ms)'

    connection.paths.return_value = []
    assert PathInfo.from_connection(connection) is None


async def test_path(managers, caplog) -> None:
    caplog.set_level(logging.INFO)
    manager1, manager2, _ = managers
    assert manager1.path(manager2.id) is None

    await _request(manager1, manager2.id, Op.GET)
    path = manager1.path(manager2.id)
    assert path is not None
    assert not path.relayed
    assert path.remote_addr.startswith('127.0.0.1:')
    messages = [r.message for r in caplog.records]
    assert any(
        f'[self({manager1.id[:10]})]: connection to peer' in m
        and 'is direct to 127.0.0.1' in m
        for m in messages
    )
    # The accepting peer also reports the path
    assert any(
        f'[self({manager2.id[:10]})]: connection from peer' in m
        and 'is direct to 127.0.0.1' in m
        for m in messages
    )

    manager1._outgoing[manager2.id].close(CloseCode.SHUTDOWN, b'close')
    assert manager1.path(manager2.id) is None


async def test_report_path_changes(managers, caplog) -> None:
    caplog.set_level(logging.INFO)
    manager1, manager2, _ = managers
    connection = mock.MagicMock()
    connection.stable_id.return_value = 42

    def _report(*paths: Any) -> list[str]:
        caplog.clear()
        connection.paths.return_value = list(paths)
        manager1._report_path(manager2.id, connection, outgoing=True)
        return [r.message for r in caplog.records]

    relay = _path(selected=True, relay=True, addr='https://relay')
    direct = _path(selected=True, relay=False, addr='1.2.3.4:5')
    assert 'relayed via https://relay' in _report(relay)[0]
    # Unchanged paths are not reported again, even if the RTT changes
    assert (
        _report(_path(selected=True, relay=True, addr='https://relay', rtt=9))
        == []
    )
    assert 'direct to 1.2.3.4:5' in _report(direct)[0]
    assert 'has no path' in _report()[0]

    manager1._drop_outgoing(manager2.id, connection)
    assert 42 not in manager1._paths


async def test_watch_path_changes(managers) -> None:
    manager1, manager2, _ = managers
    connection = mock.MagicMock()
    connection.close_reason.side_effect = [None, 'closed']
    with (
        mock.patch(_MANAGER + '._PATH_WATCH_INTERVAL', 0),
        mock.patch.object(manager1, '_report_path') as report,
    ):
        await manager1._watch_path_changes(
            manager2.id,
            connection,
            outgoing=True,
        )
    # Reported once then stopped when the connection closed
    assert report.call_count == 1

    connection.close_reason.side_effect = None
    connection.close_reason.return_value = None
    with (
        mock.patch(_MANAGER + '._PATH_WATCH_INTERVAL', 0),
        mock.patch(_MANAGER + '._PATH_WATCH_DURATION', 0.01),
        mock.patch.object(manager1, '_report_path'),
    ):
        # Stops after the watch duration
        await manager1._watch_path_changes(
            manager2.id,
            connection,
            outgoing=True,
        )

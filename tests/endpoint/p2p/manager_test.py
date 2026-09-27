from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import os
import pathlib
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import iroh
import pytest

from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.exceptions import PeerUnavailableError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.p2p.addrs import PeerAddrCache
from proxystore.endpoint.p2p.manager import _read_message
from proxystore.endpoint.p2p.manager import CloseCode
from proxystore.endpoint.p2p.manager import PathInfo
from proxystore.endpoint.p2p.manager import PeerConnection
from proxystore.endpoint.p2p.manager import PeerManager
from proxystore.endpoint.p2p.manager import PeerOptions
from proxystore.endpoint.protocol import alpn
from proxystore.endpoint.protocol import Header
from proxystore.endpoint.protocol import MAX_META_SIZE
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import MessageReader
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PROTOCOL_VERSION
from proxystore.endpoint.protocol import Status
from testing.endpoint import decode_meta
from testing.endpoint import encode_meta
from testing.p2p import allow_peer
from testing.p2p import connect_peers
from testing.p2p import local_peer_manager
from testing.p2p import LOCAL_PEER_OPTIONS
from testing.p2p import policy
from testing.utils import wait_until

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
        meta = decode_meta(request.meta)
        handled.append((peer, request.code, meta, request.data))
        if meta.get('raise'):
            raise RuntimeError('handler failed')
        echo = encode_meta({'echo': meta})
        return Message(Status.OK, echo, request.data)

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
        Message(code, b'' if meta is None else encode_meta(meta), data),
    )
    return response.code, decode_meta(response.meta), response.data


@pytest.fixture
async def managers() -> AsyncGenerator[
    tuple[PeerManager, PeerManager, Handled], None
]:
    handled: Handled = []
    manager1 = local_peer_manager()
    manager2 = local_peer_manager(
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
    assert len(manager1._connections[manager2.id]) == 1


async def test_large_request(managers) -> None:
    manager1, _, _ = managers
    manager3 = local_peer_manager()
    await manager3.start(_echo_handler([]))
    try:
        connect_peers(manager1, manager3)
        data = os.urandom(1000)
        # Use small sizes to test data spanning multiple chunks
        with (
            mock.patch(f'{_MANAGER}._CHUNK_SIZE', 300),
            mock.patch(f'{_MANAGER}._SMALL_SIZE', 100),
        ):
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
    stream = await connection.connection.open_bi()
    # Header with metadata which exceeds the maximum size
    header = Header(Op.GET, 0, 0, MAX_META_SIZE + 1, 0).pack()
    await stream.send().write_all(header)
    await stream.send().finish()
    response = await stream.recv().read_to_end(1000)
    assert response[0] == Status.BAD_REQUEST


async def test_request_not_in_allowlist(managers) -> None:
    manager1, manager2, _ = managers
    policy(manager1).peers.clear()
    with pytest.raises(PeerNotAllowedError, match='not in the allowlist'):
        await _request(manager1, manager2.id, Op.GET)


async def test_peer_refuses_connection(managers) -> None:
    manager1, manager2, handled = managers
    policy(manager2).peers.clear()
    with pytest.raises(PeerNotAllowedError, match='refused the connection'):
        await _request(manager1, manager2.id, Op.GET)
    assert len(handled) == 0


async def test_revoke_peer(managers) -> None:
    manager1, manager2, handled = managers
    await _request(manager1, manager2.id, Op.GET)
    assert manager1.id in manager2._connections

    # Removing the peer closes its connections and denies new requests.
    policy(manager2).peers.clear()
    with pytest.raises(PeerNotAllowedError):
        await _request(manager1, manager2.id, Op.GET)
    assert manager1.id not in manager2._connections
    assert len(handled) == 1

    # Adding the peer back allows it to reconnect.
    allow_peer(manager2, manager1, 'peer')
    status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK


async def test_revoke_detected_on_request(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    await _request(manager2, manager1.id, Op.GET)
    assert manager2.id in manager1._preferred

    # Manager 1 closes its connections to the removed peer the next time it
    # checks the allowlist.
    policy(manager1).peers.clear()
    with pytest.raises(PeerNotAllowedError):
        await _request(manager1, manager2.id, Op.GET)
    assert manager2.id not in manager1._preferred
    assert manager2.id not in manager1._connections


async def test_stale_connection_retry(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    stale = manager1._preferred[manager2.id]

    # Simulate the peer restarting by closing the connection from the peer.
    for connection in manager2._connections[manager1.id]:
        connection.connection.close(CloseCode.SHUTDOWN, b'restart')
    await stale.connection.closed()
    # The manager stops using a closed connection once it notices, but a
    # request can race with the closure so the connection is used again.
    await wait_until(lambda: manager2.id not in manager1._preferred)
    manager1._preferred[manager2.id] = stale

    # Mark the stale connection as open so the manager tries to use it.
    with mock.patch.object(
        type(stale.connection),
        'close_reason',
        side_effect=[None, 'closed by peer: restart (code 0)'],
    ):
        status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK
    assert manager1._preferred[manager2.id] is not stale


async def test_request_fails_on_fresh_connection(managers) -> None:
    manager1, manager2, _ = managers

    async def _fail(*args: Any) -> Any:
        raise _IrohError

    with (
        mock.patch.object(manager1, '_exchange', side_effect=_fail),
        pytest.raises(PeerUnavailableError, match='boom'),
    ):
        await _request(manager1, manager2.id, Op.GET)


async def test_connect_error() -> None:
    manager1 = local_peer_manager()
    manager2 = local_peer_manager()
    await manager1.start(_echo_handler([]))
    try:
        allow_peer(manager1, manager2, 'peer')
        # No address or discovery for the peer
        with pytest.raises(PeerUnavailableError, match='Failed to connect'):
            await _request(manager1, manager2.id, Op.GET)
    finally:
        await manager1.close()


async def test_connect_falls_back_to_older_version(managers, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    manager1, manager2, _ = managers
    newer = [alpn(PROTOCOL_VERSION + 1), alpn(PROTOCOL_VERSION)]
    # Manager 1 also supports a newer version which manager 2 does not
    with mock.patch(f'{_MANAGER}.supported_alpns', return_value=newer):
        status, _, _ = await _request(manager1, manager2.id, Op.GET)
    assert status == Status.OK
    assert manager1._preferred[manager2.id].version == PROTOCOL_VERSION
    assert any(
        'does not support protocol' in r.message for r in caplog.records
    )


async def test_connect_no_common_version(managers) -> None:
    manager1, manager2, _ = managers
    newer = [alpn(PROTOCOL_VERSION + 1)]
    with (
        mock.patch(f'{_MANAGER}.supported_alpns', return_value=newer),
        pytest.raises(PeerUnavailableError, match='none of the protocol'),
    ):
        await _request(manager1, manager2.id, Op.GET)


async def test_connect_timeout() -> None:
    manager1 = local_peer_manager(
        options=dataclasses.replace(LOCAL_PEER_OPTIONS, connect_timeout=0.1),
    )
    manager2 = local_peer_manager()
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


async def test_handle_incoming_accept_error(managers, caplog) -> None:
    caplog.set_level(logging.DEBUG, logger=_MANAGER)
    manager1, _, _ = managers
    incoming = mock.AsyncMock()
    incoming.accept.side_effect = _IrohError()
    await manager1._handle_incoming(incoming)
    assert len(manager1._connections) == 0
    assert any(
        'Failed to accept connection: boom' in r.message
        for r in caplog.records
    )


async def test_stream_error_is_logged(managers, caplog) -> None:
    caplog.set_level(logging.DEBUG, logger=_MANAGER)
    manager1, manager2, _ = managers
    stream = mock.MagicMock()
    stream.recv.return_value.read = mock.AsyncMock(side_effect=_IrohError())
    await manager1._handle_stream(manager2.id, stream)
    assert any(
        'Stream from' in r.message and 'failed: boom' in r.message
        for r in caplog.records
    )


def _mock_recv(first_read: bytes, *read_exact: bytes) -> mock.MagicMock:
    recv = mock.MagicMock()
    recv.read = mock.AsyncMock(return_value=first_read)
    recv.read_exact = mock.AsyncMock(side_effect=read_exact)
    return recv


async def test_read_message_large_data_without_prefix() -> None:
    data = os.urandom(200)
    head = Message(Op.SET, encode_meta({}), data).pack_head()
    recv = _mock_recv(head, data)
    with mock.patch(f'{_MANAGER}._SMALL_SIZE', 100):
        message = await _read_message(recv, MessageReader())
    # The data read by the bindings is used without being copied.
    assert message.data is data
    recv.read_exact.assert_awaited_once_with(len(data))


async def test_read_message_large_data_with_prefix() -> None:
    data = os.urandom(200)
    head = Message(Op.SET, encode_meta({}), data).pack_head()
    # The first read includes part of the data.
    recv = _mock_recv(head + data[:50], data[50:150], data[150:])
    with (
        mock.patch(f'{_MANAGER}._SMALL_SIZE', 100),
        mock.patch(f'{_MANAGER}._CHUNK_SIZE', 100),
    ):
        message = await _read_message(recv, MessageReader())
    assert message.data == data
    assert [c.args for c in recv.read_exact.await_args_list] == [(100,), (50,)]


async def test_stream_ends_early(managers, caplog) -> None:
    caplog.set_level(logging.DEBUG, logger=_MANAGER)
    manager1, manager2, _ = managers
    stream = mock.MagicMock()
    head = Message(Op.EXISTS, encode_meta({})).pack_head()
    # The stream ends after part of the header, so the rest cannot be read.
    stream.recv.return_value.read = mock.AsyncMock(
        side_effect=[head[:4], b''],
    )
    stream.recv.return_value.read_exact = mock.AsyncMock(
        side_effect=_IrohError(),
    )
    await manager1._handle_stream(manager2.id, stream)
    stream.recv.return_value.read_exact.assert_awaited_once_with(
        Header.SIZE - 4,
    )
    assert any(
        'Stream from' in r.message and 'failed: boom' in r.message
        for r in caplog.records
    )


async def test_not_started() -> None:
    manager = local_peer_manager()
    with pytest.raises(RuntimeError, match='not been started'):
        manager.addr()
    # Closing a manager that was never started is okay
    await manager.close()


async def test_start_and_close_idempotent() -> None:
    manager = local_peer_manager()
    await manager.start(_echo_handler([]))
    endpoint = manager.endpoint
    await manager.start(_echo_handler([]))
    assert manager.endpoint is endpoint
    await manager.close()
    await manager.close()


async def test_spawned_task_error_is_logged(managers, caplog) -> None:
    manager1, _, _ = managers

    async def _fail() -> None:
        raise RuntimeError('task failed')

    before = set(manager1._tasks)
    manager1._spawn(_fail())
    (task,) = manager1._tasks - before
    with pytest.raises(RuntimeError):
        await task
    # Done callbacks are scheduled after the task completes.
    await asyncio.sleep(0)
    assert task not in manager1._tasks
    records = [r for r in caplog.records if 'Unexpected error' in r.message]
    assert len(records) == 1
    assert '_fail' in records[0].message
    assert records[0].exc_info is not None
    assert 'task failed' in str(records[0].exc_info[1])


@pytest.mark.parametrize(
    ('online', 'timeout', 'message'),
    (
        (mock.AsyncMock(), 1, 'Connected to home relay'),
        (
            mock.AsyncMock(side_effect=RuntimeError('online failed')),
            1,
            'Failed to wait for a home relay',
        ),
        # Relays are disabled so the endpoint never connects to a home relay
        (None, 0.01, 'Not connected to a home relay'),
    ),
)
async def test_online(
    online: mock.AsyncMock | None,
    timeout: float,
    message: str,
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    manager = local_peer_manager(
        options=dataclasses.replace(
            LOCAL_PEER_OPTIONS, online_timeout=timeout
        ),
    )
    patch = (
        contextlib.nullcontext()
        if online is None
        else mock.patch.object(iroh.Endpoint, 'online', online)
    )
    with patch:
        await manager.start(_echo_handler([]))
        assert manager._online_task is not None
        await manager._online_task
    # Errors do not stop the manager from closing
    await manager.close()
    assert manager.endpoint.is_closed()
    assert any(message in r.message for r in caplog.records)


async def test_drop_replaced_preferred_connection(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    current = manager1._preferred[manager2.id]
    # Dropping a connection that was already replaced keeps the current one.
    manager1._drop_preferred(
        PeerConnection(manager2.id, mock.MagicMock(), True)
    )
    assert manager1._preferred[manager2.id] is current


@pytest.mark.parametrize(
    ('write_fails', 'read_fails'),
    ((True, False), (True, True), (False, True)),
)
async def test_exchange_errors(
    managers,
    write_fails: bool,
    read_fails: bool,
) -> None:
    manager1, _, _ = managers
    connection = mock.AsyncMock()
    connection.open_bi.return_value = mock.MagicMock()
    write_error = _IrohError()
    read_error = _IrohError()
    # The peer stops reading a rejected request but still responds
    expected = Message.error(Status.TOO_LARGE, 'too large')
    with (
        mock.patch(
            f'{_MANAGER}._write_message',
            side_effect=write_error if write_fails else None,
        ),
        mock.patch(
            f'{_MANAGER}._read_message',
            side_effect=read_error if read_fails else None,
            return_value=expected,
        ),
    ):
        request = Message(Op.SET, data=b'x')
        if not read_fails:
            assert await manager1._exchange(connection, request) == expected
            return
        with pytest.raises(_IrohError) as exc_info:
            await manager1._exchange(connection, request)
    # The write error is raised if both fail because it is the cause
    assert exc_info.value is (write_error if write_fails else read_error)


async def test_addr_cache(tmp_path: pathlib.Path) -> None:
    cache1 = str(tmp_path / 'addrs1.json')
    cache2 = str(tmp_path / 'addrs2.json')
    manager1 = local_peer_manager(
        addr_cache=PeerAddrCache(cache1),
    )
    manager2 = local_peer_manager(
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
    removed = local_peer_manager()
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
    assert any('Failed to save' in r.message for r in caplog.records)


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
        f'Connection to peer {manager1.peer_name(manager2.id)}' in m
        and 'is direct to 127.0.0.1' in m
        for m in messages
    )
    # The accepting peer also reports the path
    assert any(
        f'Connection from peer {manager2.peer_name(manager1.id)}' in m
        and 'is direct to 127.0.0.1' in m
        for m in messages
    )

    manager1._preferred[manager2.id].connection.close(
        CloseCode.SHUTDOWN,
        b'close',
    )
    assert manager1.path(manager2.id) is None


def test_report_path_changes(caplog) -> None:
    caplog.set_level(logging.INFO)
    iroh_connection = mock.MagicMock()
    connection = PeerConnection(EndpointId.random(), iroh_connection, True)

    def _report(*paths: Any) -> list[str]:
        caplog.clear()
        iroh_connection.paths.return_value = list(paths)
        connection.report_path('peer')
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
    assert 'Connection to peer peer' in _report(direct)[0]

    iroh_connection.paths.return_value = [direct]
    accepted = PeerConnection(EndpointId.random(), iroh_connection, False)
    caplog.clear()
    accepted.report_path('peer')
    assert 'Connection from peer peer' in caplog.records[0].message


async def test_watch_path() -> None:
    iroh_connection = mock.MagicMock()
    iroh_connection.close_reason.side_effect = [None, 'closed']
    connection = PeerConnection(EndpointId.random(), iroh_connection, True)
    with (
        mock.patch(_MANAGER + '._PATH_WATCH_INTERVAL', 0),
        mock.patch.object(connection, 'report_path') as report,
    ):
        await connection.watch_path('peer')
    # Reported once then stopped when the connection closed
    assert report.call_count == 1

    iroh_connection.close_reason.side_effect = None
    iroh_connection.close_reason.return_value = None
    with (
        mock.patch(_MANAGER + '._PATH_WATCH_INTERVAL', 0),
        mock.patch(_MANAGER + '._PATH_WATCH_DURATION', 0.01),
        mock.patch.object(connection, 'report_path'),
    ):
        # Stops after the watch duration
        await connection.watch_path('peer')


async def test_connection_used_in_both_directions(managers) -> None:
    manager1, manager2, handled = managers
    await _request(manager1, manager2.id, Op.GET)
    # Manager 2 sends requests on the connection opened by manager 1
    with mock.patch.object(manager2, '_dial') as dial:
        status, _, _ = await _request(manager2, manager1.id, Op.GET)
    assert status == Status.OK
    dial.assert_not_called()
    assert len(manager1._connections[manager2.id]) == 1
    assert len(manager2._connections[manager1.id]) == 1
    assert len(handled) == 2
    # The path of the connection is known in both directions
    assert manager1.path(manager2.id) is not None
    assert manager2.path(manager1.id) is not None


async def test_simultaneous_connections(managers) -> None:
    manager1, manager2, _ = managers
    results = await asyncio.gather(
        _request(manager1, manager2.id, Op.GET),
        _request(manager2, manager1.id, Op.GET),
    )
    assert all(status == Status.OK for status, _, _ in results)
    # Each peer may have opened a connection, and both remain usable.
    assert 1 <= len(manager1._connections[manager2.id]) <= 2
    for _ in range(3):
        status, _, _ = await _request(manager1, manager2.id, Op.GET)
        assert status == Status.OK
        status, _, _ = await _request(manager2, manager1.id, Op.GET)
        assert status == Status.OK


async def test_close_notifies_peers(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    connection = manager1._preferred[manager2.id].connection

    await manager2.close()
    await connection.closed()
    assert 'shutdown' in str(connection.close_reason())


async def test_closed_connection_is_forgotten(managers) -> None:
    manager1, manager2, _ = managers
    await _request(manager1, manager2.id, Op.GET)
    connection = manager1._preferred[manager2.id]
    assert connection in manager1._connections[manager2.id]

    connection.connection.close(CloseCode.SHUTDOWN, b'close')
    await wait_until(lambda: manager2.id not in manager1._connections)
    assert manager2.id not in manager1._preferred


@pytest.mark.parametrize(
    'relays',
    ('n0', 'none', ['https://relay.example.com']),
)
@pytest.mark.parametrize('discovery', ('n0', 'none'))
def test_peer_options_from_config(relays: Any, discovery: Any) -> None:
    config = EndpointP2PConfig(relays=relays, discovery=discovery)
    with (
        mock.patch('iroh.preset_n0', wraps=iroh.preset_n0) as n0,
        mock.patch(
            'iroh.preset_minimal', wraps=iroh.preset_minimal
        ) as minimal,
    ):
        options = PeerOptions.from_config(config)
    assert isinstance(options.preset, iroh.Preset)
    assert n0.called == (discovery == 'n0')
    assert minimal.called == (discovery == 'none')
    if relays == 'n0' and discovery == 'n0':
        # The relays of the n0 preset are used
        assert options.relay_mode is None
    else:
        assert isinstance(options.relay_mode, iroh.RelayMode)
    assert (options.online_timeout is None) == (relays == 'none')

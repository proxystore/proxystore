from __future__ import annotations

import asyncio
import logging
import os
import pathlib
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import iroh
import pytest

from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import Status
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerConnectionTimeoutError
from proxystore.p2p.exceptions import PeerNotAllowedError
from proxystore.p2p.manager import _closed_with
from proxystore.p2p.manager import CloseCode
from proxystore.p2p.manager import PeerManager
from testing.p2p import allow_peer
from testing.p2p import connect_peers
from testing.p2p import local_peer_manager


class _IrohError(iroh.IrohError):
    # IrohError has no default constructor.
    def __init__(self) -> None:
        pass

    def message(self) -> str:
        return 'boom'


Handled = list[tuple[EndpointId, int, dict[str, Any], bytes | bytearray]]


def _echo_handler(handled: Handled) -> Any:
    async def _handler(
        peer: EndpointId,
        code: int,
        meta: dict[str, Any],
        data: bytes | bytearray,
    ) -> tuple[int, dict[str, Any] | None, bytes | bytearray | None]:
        handled.append((peer, code, meta, data))
        if meta.get('raise'):
            raise RuntimeError('handler failed')
        return Status.OK, {'echo': meta}, data

    return _handler


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

    status, meta, data = await manager1.request(
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
    status, meta, data = await manager2.request(manager1.id, Op.EXISTS)
    assert status == Status.OK
    assert meta == {'echo': {}}
    assert data == b''


async def test_concurrent_requests(managers) -> None:
    manager1, manager2, _ = managers
    results = await asyncio.gather(
        *(
            manager1.request(manager2.id, Op.GET, {'i': i}, bytes([i]))
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
        with mock.patch('proxystore.p2p.manager._CHUNK_SIZE', 300):
            _, _, response = await manager1.request(
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
    status, meta, _ = await manager1.request(
        manager2.id,
        Op.SET,
        {},
        b'x' * 1001,
    )
    assert status == Status.TOO_LARGE
    assert 'exceeds the maximum object size' in meta['error']
    assert len(handled) == 0

    # Connection is still usable
    status, _, _ = await manager1.request(manager2.id, Op.SET, {}, b'x')
    assert status == Status.OK


async def test_handler_error(managers) -> None:
    manager1, manager2, _ = managers
    status, meta, _ = await manager1.request(
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
    await stream.send().write_all(bytes([Op.GET, 0, 0, 0, 0, 1] + [0] * 8))
    await stream.send().write_all(b'x')
    await stream.send().finish()
    response = await stream.recv().read_to_end(1000)
    assert response[0] == Status.BAD_REQUEST


async def test_request_not_in_allowlist(managers) -> None:
    manager1, manager2, _ = managers
    os.remove(manager1._allowlist.path)
    with pytest.raises(PeerNotAllowedError, match='not in the allowlist'):
        await manager1.request(manager2.id, Op.GET)


async def test_peer_refuses_connection(managers) -> None:
    manager1, manager2, handled = managers
    os.remove(manager2._allowlist.path)
    with pytest.raises(PeerNotAllowedError, match='refused the connection'):
        await manager1.request(manager2.id, Op.GET)
    assert len(handled) == 0


async def test_revoke_peer(managers) -> None:
    manager1, manager2, handled = managers
    await manager1.request(manager2.id, Op.GET)
    assert manager1.id in manager2._incoming

    # Removing the peer closes its connections and denies new requests.
    os.remove(manager2._allowlist.path)
    with pytest.raises(PeerNotAllowedError):
        await manager1.request(manager2.id, Op.GET)
    assert manager1.id not in manager2._incoming
    assert len(handled) == 1

    # Adding the peer back allows it to reconnect.
    allow_peer(manager2, manager1, 'peer')
    status, _, _ = await manager1.request(manager2.id, Op.GET)
    assert status == Status.OK


async def test_revoke_detected_on_request(managers) -> None:
    manager1, manager2, _ = managers
    await manager1.request(manager2.id, Op.GET)
    await manager2.request(manager1.id, Op.GET)
    assert manager2.id in manager1._outgoing

    # Manager 1 closes its connections to the removed peer the next time it
    # checks the allowlist.
    os.remove(manager1._allowlist.path)
    with pytest.raises(PeerNotAllowedError):
        await manager1.request(manager2.id, Op.GET)
    assert manager2.id not in manager1._outgoing
    assert manager2.id not in manager1._incoming


async def test_stale_connection_retry(managers, tmp_path) -> None:
    manager1, manager2, _ = managers
    await manager1.request(manager2.id, Op.GET)
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
        status, _, _ = await manager1.request(manager2.id, Op.GET)
    assert status == Status.OK
    assert manager1._outgoing[manager2.id] is not stale


async def test_request_fails_on_fresh_connection(managers) -> None:
    manager1, manager2, _ = managers

    async def _fail(*args: Any) -> Any:
        raise _IrohError

    with (
        mock.patch.object(manager1, '_exchange', side_effect=_fail),
        pytest.raises(PeerConnectionError, match='boom'),
    ):
        await manager1.request(manager2.id, Op.GET)


async def test_connect_error(tmp_path: pathlib.Path) -> None:
    manager1 = local_peer_manager(str(tmp_path / 'm1'))
    manager2 = local_peer_manager(str(tmp_path / 'm2'))
    await manager1.start(_echo_handler([]))
    try:
        allow_peer(manager1, manager2, 'peer')
        # No address or discovery for the peer
        with pytest.raises(PeerConnectionError, match='Failed to connect'):
            await manager1.request(manager2.id, Op.GET)
    finally:
        await manager1.close()


async def test_connect_timeout(tmp_path: pathlib.Path) -> None:
    manager1 = local_peer_manager(str(tmp_path / 'm1'), connect_timeout=0.1)
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
            await manager1.request(manager2.id, Op.GET)
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
    manager = local_peer_manager(str(tmp_path), online_timeout=1)
    with mock.patch.object(iroh.Endpoint, 'online', mock.AsyncMock()):
        await manager.start(_echo_handler([]))
        assert manager._online_task is not None
        await manager._online_task
    await manager.close()
    assert any('connected to home relay' in r.message for r in caplog.records)


def test_closed_with() -> None:
    reason = 'closed by peer: not allowed (code 1)'
    assert _closed_with(reason, CloseCode.NOT_ALLOWED)
    assert not _closed_with(reason, CloseCode.SHUTDOWN)


async def test_drop_outgoing_replaced_connection(managers) -> None:
    manager1, manager2, _ = managers
    await manager1.request(manager2.id, Op.GET)
    current = manager1._outgoing[manager2.id]
    # Dropping a connection that was already replaced keeps the current one.
    manager1._drop_outgoing(manager2.id, mock.MagicMock())
    assert manager1._outgoing[manager2.id] is current


async def test_exchange_write_error_reads_response(managers) -> None:
    manager1, _, _ = managers
    connection = mock.AsyncMock()
    connection.open_bi.return_value = mock.MagicMock()
    header = mock.MagicMock(code=Status.TOO_LARGE)
    with (
        mock.patch(
            'proxystore.p2p.manager._write_message',
            side_effect=_IrohError(),
        ),
        mock.patch(
            'proxystore.p2p.manager._read_message',
            return_value=(header, {'error': 'too large'}, b''),
        ),
    ):
        response = await manager1._exchange(connection, Op.SET, None, b'x')
    assert response == (Status.TOO_LARGE, {'error': 'too large'}, b'')


@pytest.mark.parametrize('write_fails', (True, False))
async def test_exchange_read_error(managers, write_fails: bool) -> None:
    manager1, _, _ = managers
    connection = mock.AsyncMock()
    connection.open_bi.return_value = mock.MagicMock()
    write_error = _IrohError()
    read_error = _IrohError()
    with (
        mock.patch(
            'proxystore.p2p.manager._write_message',
            side_effect=write_error if write_fails else None,
        ),
        mock.patch(
            'proxystore.p2p.manager._read_message',
            side_effect=read_error,
        ),
        pytest.raises(_IrohError) as exc_info,
    ):
        await manager1._exchange(connection, Op.SET, None, b'x')
    assert exc_info.value is (write_error if write_fails else read_error)

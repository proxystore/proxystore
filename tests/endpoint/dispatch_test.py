from __future__ import annotations

import os
import pathlib
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.dispatch import Dispatcher
from proxystore.endpoint.exceptions import PeerConnectionTimeoutError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import exists_from_meta
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.storage import DictStorage
from testing.compat import randbytes
from testing.p2p import connect_peers
from testing.p2p import local_peer_manager


def _request(
    op: Op,
    key: str | None = 'key',
    target: EndpointId | None = None,
    data: bytes = b'',
) -> Message:
    return Message(op, Request(key, target).to_meta(), data)


async def _handle(
    dispatcher: Dispatcher, *args: Any, **kwargs: Any
) -> Message:
    return await dispatcher.handle(_request(*args, **kwargs), forward=True)


@pytest.fixture
async def dispatcher() -> AsyncGenerator[Dispatcher, None]:
    storage = DictStorage()
    yield Dispatcher(EndpointId.random(), storage)
    await storage.close()


@pytest.fixture
async def peers(
    tmp_path: pathlib.Path,
) -> AsyncGenerator[tuple[Dispatcher, Dispatcher], None]:
    manager1 = local_peer_manager(str(tmp_path / 'ep1'))
    manager2 = local_peer_manager(str(tmp_path / 'ep2'))
    dispatcher1 = Dispatcher(manager1.id, DictStorage(), manager1)
    dispatcher2 = Dispatcher(
        manager2.id,
        DictStorage(max_object_size=100),
        manager2,
    )
    await manager1.start(dispatcher1.handle_peer_request)
    await manager2.start(dispatcher2.handle_peer_request)
    connect_peers(manager1, manager2)
    try:
        yield dispatcher1, dispatcher2
    finally:
        await manager1.close()
        await manager2.close()


async def test_local_operations(dispatcher: Dispatcher) -> None:
    data = randbytes(100)
    response = await _handle(dispatcher, Op.EXISTS)
    assert response == Message(Status.OK, {'exists': False})
    assert (await _handle(dispatcher, Op.GET)).code == Status.NOT_FOUND

    assert await _handle(dispatcher, Op.SET, data=data) == Message(Status.OK)
    assert await dispatcher.storage.get('key') == data
    assert exists_from_meta((await _handle(dispatcher, Op.EXISTS)).meta)
    response = await _handle(dispatcher, Op.GET)
    assert response == Message(Status.OK, data=data)

    # Requests which target this endpoint are not forwarded
    response = await _handle(dispatcher, Op.GET, target=dispatcher.id)
    assert response == Message(Status.OK, data=data)

    assert await _handle(dispatcher, Op.EVICT) == Message(Status.OK)
    assert not await dispatcher.storage.exists('key')
    # Evicting a missing key is not an error
    assert await _handle(dispatcher, Op.EVICT) == Message(Status.OK)


async def test_local_ping(dispatcher: Dispatcher) -> None:
    response = await _handle(dispatcher, Op.PING, key=None)
    assert PingResult.from_meta(response.meta) == PingResult()


@pytest.mark.parametrize('op', (Op.GET, Op.SET, Op.EXISTS, Op.EVICT, Op.PING))
async def test_peering_disabled(dispatcher: Dispatcher, op: Op) -> None:
    response = await _handle(dispatcher, op, target=EndpointId.random())
    assert response.code == Status.PEERING_DISABLED
    assert 'peering is disabled' in response.meta['error']


async def test_bad_requests(dispatcher: Dispatcher) -> None:
    response = await dispatcher.handle(Message(Op.GET, {}), forward=True)
    assert response.code == Status.BAD_REQUEST
    assert "invalid 'key'" in response.meta['error']

    response = await _handle(dispatcher, Op.GET, key=None)
    assert response == Message.error(
        Status.BAD_REQUEST,
        'Request requires a key.',
    )

    response = await _handle(dispatcher, 42)
    assert response == Message.error(Status.BAD_REQUEST, 'unknown op 42')

    # Handshake messages are not requests
    response = await _handle(dispatcher, Op.HELLO)
    assert response == Message.error(Status.BAD_REQUEST, 'unknown op 1')


async def test_peer_requests_are_not_forwarded(
    dispatcher: Dispatcher,
) -> None:
    request = _request(Op.GET, target=EndpointId.random())
    response = await dispatcher.handle_peer_request(
        EndpointId.random(),
        request,
    )
    assert response == Message.error(
        Status.BAD_REQUEST,
        'requests from peers cannot be forwarded',
    )


async def test_unexpected_error(dispatcher: Dispatcher) -> None:
    with mock.patch.object(
        dispatcher.storage,
        'get',
        side_effect=RuntimeError('storage failed'),
    ):
        response = await _handle(dispatcher, Op.GET)
    assert response.code == Status.ERROR
    assert 'storage failed' in response.meta['error']


async def test_mismatched_peer_manager_id(tmp_path: pathlib.Path) -> None:
    manager = local_peer_manager(str(tmp_path))
    with pytest.raises(ValueError, match='does not match'):
        Dispatcher(EndpointId.random(), DictStorage(), manager)


async def test_forward_operations(peers) -> None:
    dispatcher1, dispatcher2 = peers
    target = dispatcher2.id
    data = randbytes(50)

    response = await _handle(dispatcher1, Op.SET, target=target, data=data)
    assert response == Message(Status.OK)
    assert await dispatcher2.storage.get('key') == data
    assert not await dispatcher1.storage.exists('key')

    response = await _handle(dispatcher1, Op.EXISTS, target=target)
    assert response == Message(Status.OK, {'exists': True})
    response = await _handle(dispatcher1, Op.GET, target=target)
    assert response.code == Status.OK
    assert response.data == data

    response = await _handle(dispatcher1, Op.EVICT, target=target)
    assert response == Message(Status.OK)
    assert not await dispatcher2.storage.exists('key')
    response = await _handle(dispatcher1, Op.GET, target=target)
    assert response.code == Status.NOT_FOUND


async def test_forward_peer_error_status(peers) -> None:
    dispatcher1, dispatcher2 = peers
    target = dispatcher2.id
    # The status of the peer is returned with the peer named in the error
    response = await _handle(
        dispatcher1,
        Op.SET,
        target=target,
        data=randbytes(101),
    )
    assert response.code == Status.TOO_LARGE
    assert response.meta['error'].startswith(f'Peer {target}: ')

    response = await _handle(dispatcher1, Op.GET, key=None, target=target)
    assert response.code == Status.BAD_REQUEST
    assert 'Request requires a key' in response.meta['error']


async def test_forward_not_allowed(peers) -> None:
    dispatcher1, dispatcher2 = peers
    assert dispatcher1.peer_manager is not None
    os.remove(dispatcher1.peer_manager._allowlist.path)
    response = await _handle(dispatcher1, Op.GET, target=dispatcher2.id)
    assert response.code == Status.PEER_NOT_ALLOWED
    assert 'not in the allowlist' in response.meta['error']


async def test_forward_peer_refused(peers) -> None:
    dispatcher1, dispatcher2 = peers
    assert dispatcher2.peer_manager is not None
    os.remove(dispatcher2.peer_manager._allowlist.path)
    response = await _handle(dispatcher1, Op.PING, target=dispatcher2.id)
    assert response.code == Status.PEER_NOT_ALLOWED
    assert 'refused the connection' in response.meta['error']


async def test_forward_peer_unavailable(peers) -> None:
    dispatcher1, dispatcher2 = peers
    assert dispatcher1.peer_manager is not None
    with mock.patch.object(
        dispatcher1.peer_manager,
        'request',
        side_effect=PeerConnectionTimeoutError('timed out'),
    ):
        response = await _handle(dispatcher1, Op.GET, target=dispatcher2.id)
    assert response == Message.error(Status.PEER_UNAVAILABLE, 'timed out')


async def test_forward_ping(peers) -> None:
    dispatcher1, dispatcher2 = peers
    response = await _handle(dispatcher1, Op.PING, key=None, target=None)
    assert PingResult.from_meta(response.meta) == PingResult()

    response = await _handle(
        dispatcher1,
        Op.PING,
        key=None,
        target=dispatcher2.id,
    )
    result = PingResult.from_meta(response.meta)
    assert result.peer_rtt_ms is not None
    assert result.peer_rtt_ms > 0
    assert result.relayed is False
    assert result.remote_addr is not None
    assert result.remote_addr.startswith('127.0.0.1:')
    assert result.path_rtt_ms is not None

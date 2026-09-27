from __future__ import annotations

import os
import pathlib
from collections.abc import AsyncGenerator
from unittest import mock

import pytest

from proxystore.endpoint.endpoint import Endpoint
from proxystore.endpoint.exceptions import ObjectSizeExceededError
from proxystore.endpoint.exceptions import PeerNotAllowedError
from proxystore.endpoint.exceptions import PeerRequestError
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.protocol import Message
from proxystore.endpoint.protocol import Op
from proxystore.endpoint.protocol import PingResult
from proxystore.endpoint.protocol import Request
from proxystore.endpoint.protocol import Status
from proxystore.endpoint.serve import peer_request_handler
from proxystore.endpoint.storage import DictStorage
from testing.compat import randbytes
from testing.p2p import connect_peers
from testing.p2p import local_peer_manager


@pytest.fixture
async def endpoints(
    tmp_path: pathlib.Path,
) -> AsyncGenerator[tuple[Endpoint, Endpoint], None]:
    manager1 = local_peer_manager(str(tmp_path / 'ep1'))
    manager2 = local_peer_manager(str(tmp_path / 'ep2'))
    async with (
        Endpoint('ep1', manager1.id, peer_manager=manager1) as ep1,
        Endpoint(
            'ep2',
            manager2.id,
            peer_manager=manager2,
            storage=DictStorage(max_object_size=100),
        ) as ep2,
    ):
        await manager1.start(peer_request_handler(ep1))
        await manager2.start(peer_request_handler(ep2))
        connect_peers(manager1, manager2)
        try:
            yield ep1, ep2
        finally:
            await manager1.close()
            await manager2.close()


async def test_init_mismatched_id(tmp_path: pathlib.Path) -> None:
    manager = local_peer_manager(str(tmp_path))
    with pytest.raises(ValueError, match='does not match'):
        Endpoint('ep', EndpointId.random(), peer_manager=manager)


async def test_close_does_not_close_peer_manager(
    tmp_path: pathlib.Path,
) -> None:
    manager = local_peer_manager(str(tmp_path))
    endpoint = await Endpoint('ep', manager.id, peer_manager=manager)
    assert endpoint.peer_manager is manager
    with mock.patch.object(manager, 'close') as close:
        await endpoint.close()
        await endpoint.close()
    # The owner of the peer manager (e.g., the EndpointService) closes it
    close.assert_not_called()


async def test_remote_operations(endpoints) -> None:
    ep1, ep2 = endpoints
    data = randbytes(50)

    await ep1.set('key', data, target=ep2.id)
    assert await ep2.get('key') == data
    assert not await ep1.exists('key')

    assert await ep1.exists('key', target=ep2.id)
    assert await ep1.get('key', target=ep2.id) == data

    await ep1.evict('key', target=ep2.id)
    assert not await ep2.exists('key')
    assert not await ep1.exists('key', target=ep2.id)
    assert await ep1.get('key', target=ep2.id) is None

    # Requests for the local endpoint are not forwarded
    await ep1.set('key', data, target=ep1.id)
    assert await ep1.get('key') == data


async def test_remote_object_too_large(endpoints) -> None:
    ep1, ep2 = endpoints
    with pytest.raises(ObjectSizeExceededError, match=f'Peer {ep2.id}'):
        await ep1.set('key', randbytes(101), target=ep2.id)


async def test_remote_not_allowed(endpoints) -> None:
    ep1, ep2 = endpoints
    assert ep1.peer_manager is not None
    os.remove(ep1.peer_manager._allowlist.path)
    with pytest.raises(PeerNotAllowedError, match='not in the allowlist'):
        await ep1.get('key', target=ep2.id)


async def test_remote_error(endpoints) -> None:
    ep1, ep2 = endpoints
    with (
        mock.patch.object(
            DictStorage,
            'get',
            side_effect=RuntimeError('storage failed'),
        ),
        pytest.raises(PeerRequestError, match='storage failed'),
    ):
        await ep1.get('key', target=ep2.id)


async def test_remote_malformed_exists(endpoints) -> None:
    ep1, ep2 = endpoints
    assert ep1.peer_manager is not None
    with (
        mock.patch.object(
            ep1.peer_manager,
            'request',
            mock.AsyncMock(return_value=Message(Status.OK)),
        ),
        pytest.raises(PeerRequestError, match='Malformed EXISTS'),
    ):
        await ep1.exists('key', target=ep2.id)


async def test_handle_peer_request_errors(endpoints) -> None:
    ep1, ep2 = endpoints
    handle = peer_request_handler(ep2)

    response = await handle(ep1.id, Message(Op.GET, {}))
    assert response.code == Status.BAD_REQUEST
    assert "invalid 'key'" in response.meta['error']

    response = await handle(ep1.id, Message(Op.GET, Request().to_meta()))
    assert response == Message.error(
        Status.BAD_REQUEST,
        'Request requires a key.',
    )

    forward = Request('key', EndpointId.random()).to_meta()
    response = await handle(ep1.id, Message(Op.GET, forward))
    assert response == Message.error(
        Status.BAD_REQUEST,
        'requests from peers cannot be forwarded',
    )

    response = await handle(ep1.id, Message(42, Request('key').to_meta()))
    assert response == Message.error(Status.BAD_REQUEST, 'unknown op 42')

    too_large = randbytes(101)
    request = Message(Op.SET, Request('k').to_meta(), too_large)
    response = await handle(ep1.id, request)
    assert response.code == Status.TOO_LARGE


async def test_ping(endpoints) -> None:
    ep1, ep2 = endpoints
    assert await ep1.ping() == PingResult()
    assert await ep1.ping(ep1.id) == PingResult()

    result = await ep1.ping(ep2.id)
    assert result.peer_rtt_ms is not None
    assert result.peer_rtt_ms > 0
    assert result.relayed is False
    assert result.remote_addr is not None
    assert result.remote_addr.startswith('127.0.0.1:')
    assert result.path_rtt_ms is not None


async def test_ping_not_allowed(endpoints) -> None:
    ep1, ep2 = endpoints
    assert ep2.peer_manager is not None
    os.remove(ep2.peer_manager._allowlist.path)
    with pytest.raises(PeerNotAllowedError, match='refused the connection'):
        await ep1.ping(ep2.id)

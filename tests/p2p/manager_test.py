from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from unittest import mock

import pytest

from proxystore.p2p import messages
from proxystore.p2p.exceptions import PeerConnectionError
from proxystore.p2p.exceptions import PeerRegistrationError
from proxystore.p2p.manager import PeerManager
from testing.mocking import async_mock_once


if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock


@pytest.mark.asyncio
async def test_awaitable(signaling_server) -> None:
    manager = await PeerManager(uuid.uuid4(), signaling_server.address)
    # Calling async_init again should do nothing
    await manager.async_init()
    await manager.close()


@pytest.mark.asyncio
async def test_not_awaited(signaling_server) -> None:
    manager = PeerManager(uuid.uuid4(), signaling_server.address)
    with pytest.raises(RuntimeError, match='await'):
        await manager.get_connection(uuid.uuid4())
    await manager.close()


@pytest.mark.asyncio
async def test_uuid_name_properties(signaling_server) -> None:
    uuid_ = uuid.uuid4()
    name = 'pm'
    async with PeerManager(
        uuid_,
        signaling_server.address,
        name=name,
    ) as manager:
        assert manager.uuid == uuid_
        assert manager.name == name


@pytest.mark.asyncio
async def test_uuid_mismatch(signaling_server) -> None:
    amock = AsyncMock(return_value=('wrong-uuid', None, None))
    with mock.patch('proxystore.p2p.manager.connect', side_effect=amock):
        with pytest.raises(PeerRegistrationError, match='non-matching UUID'):
            await PeerManager(uuid.uuid4(), signaling_server.address)


@pytest.mark.asyncio
async def test_p2p_connection(signaling_server) -> None:
    async with PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager1, PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager2:
        connection1 = await manager1.get_connection(manager2.uuid)
        assert connection1 == await manager1.get_connection(manager2.uuid)
        await connection1.ready()
        assert connection1.state == 'connected'

        connection2 = await manager2.get_connection(manager1.uuid)
        await connection2.ready()
        assert connection2.state == 'connected'


@pytest.mark.asyncio
async def test_p2p_connection_error_unknown_peer(signaling_server) -> None:
    async with PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager1:
        with pytest.raises(PeerConnectionError, match='unknown'):
            await manager1.send(
                uuid.uuid4(),
                messages.Message(),  # type: ignore
                timeout=0.2,
            )


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason='aiortc in py37 raises InvalidStateError for unknown reason',
)
async def test_p2p_connection_error_from_server(
    signaling_server,
) -> None:  # pragma: >=3.8 cover
    async with PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager1, PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager2:
        # Mock manager 1 to receive error peer connection from
        # signaling server
        assert manager1._websocket_or_none is not None
        mock_recv = async_mock_once(
            manager1._websocket_or_none.recv,
            messages.encode(
                messages.PeerConnection(
                    source_uuid=manager2.uuid,
                    source_name=manager2.name,
                    peer_uuid=manager1.uuid,
                    description_type='offer',
                    description='',
                    error='test error',
                ),
            ),
        )
        manager1._websocket_or_none.recv = mock_recv  # type: ignore

        connection1 = await manager1.get_connection(manager2.uuid)

        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

        with pytest.raises(PeerConnectionError, match='test error'):
            await connection1.ready()


@pytest.mark.asyncio
async def test_p2p_messaging(signaling_server) -> None:
    async with PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager1, PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager2:
        await manager1.send(
            manager2.uuid,
            messages.PeerMessage(
                source_uuid=manager1.uuid,
                peer_uuid=manager2.uuid,
                message='hello hello',
            ),
        )
        message = await manager2.recv()
        assert message.source_uuid == manager1.uuid
        assert message.message == 'hello hello'


@pytest.mark.asyncio
async def test_p2p_messaging_error(signaling_server, caplog) -> None:
    caplog.set_level(logging.ERROR)
    async with PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager1, PeerManager(
        uuid.uuid4(),
        signaling_server.address,
    ) as manager2:
        await manager1.send(
            manager2.uuid,
            messages.ServerRegistration('name', uuid.uuid4()),  # type: ignore
        )
        # should raise timeout because sent message is not valid
        # and will not be put on received message queue
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(manager2.recv(), 0.2)

        assert any(
            [
                'non-peer message type' in record.message
                and record.levelname == 'ERROR'
                for record in caplog.records
            ],
        )


@pytest.mark.asyncio
async def test_expected_server_disconnect(signaling_server) -> None:
    manager = await PeerManager(uuid.uuid4(), signaling_server.address)
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await signaling_server.signaling_server._uuid_to_client[
        manager.uuid
    ].websocket.close()
    await manager.close()


@pytest.mark.asyncio
async def test_unexpected_server_disconnect(signaling_server) -> None:
    manager = await PeerManager(uuid.uuid4(), signaling_server.address)
    # TODO(gpauloski): should we log something or set a flag in the manager?
    await signaling_server.signaling_server._uuid_to_client[
        manager.uuid
    ].websocket.close(code=1002)
    await manager.close()


@pytest.mark.asyncio
async def test_serialization_error(signaling_server, caplog) -> None:
    # PeerManager should log an error and skip the message but
    # not raise an exception.
    caplog.set_level(logging.ERROR)
    async with PeerManager(uuid.uuid4(), signaling_server.address) as manager:
        assert manager._websocket_or_none is not None
        mock_recv = async_mock_once(
            manager._websocket_or_none.recv,
            'nonsense_string',
        )
        manager._websocket_or_none.recv = mock_recv  # type: ignore
        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'error deserializing' in record.message
            and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio
async def test_unexpected_server_response(signaling_server, caplog) -> None:
    # PeerManager should log an exception and skip the message but
    # not raise an exception.
    caplog.set_level(logging.ERROR)
    async with PeerManager(uuid.uuid4(), signaling_server.address) as manager:
        assert manager._websocket_or_none is not None
        mock_recv = async_mock_once(
            manager._websocket_or_none.recv,
            messages.encode(
                messages.ServerResponse(success=True, message='', error=False),
            ),
        )
        manager._websocket_or_none.recv = mock_recv  # type: ignore
        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'got unexpected' in record.message and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio
async def test_unknown_message_type(signaling_server, caplog) -> None:
    # PeerManager should log an error and skip the message but
    # not raise an exception.
    caplog.set_level(logging.ERROR)
    async with PeerManager(uuid.uuid4(), signaling_server.address) as manager:
        assert manager._websocket_or_none is not None
        mock_recv = async_mock_once(
            manager._websocket_or_none.recv,
            messages.encode(messages.ServerRegistration('name', uuid.uuid4())),
        )
        manager._websocket_or_none.recv = mock_recv  # type: ignore
        while not mock_recv.await_count > 1:
            await asyncio.sleep(0.01)

    assert any(
        [
            'unknown message' in record.message and record.levelname == 'ERROR'
            for record in caplog.records
        ],
    )

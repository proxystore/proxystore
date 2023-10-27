from __future__ import annotations

import asyncio
import logging
import uuid
from unittest import mock
from unittest.mock import AsyncMock

import pytest
import websockets

from proxystore.p2p.relay.authenticate import NullAuthenticator
from proxystore.p2p.relay.authenticate import NullUser
from proxystore.p2p.relay.exceptions import BadRequestError
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError
from proxystore.p2p.relay.manager import Client
from proxystore.p2p.relay.messages import decode_relay_message
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayMessage
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse
from proxystore.p2p.relay.server import RelayServer
from testing.relay_server import RelayServerInfo

_WAIT_FOR = 0.2


def get_mock_websocket() -> websockets.server.WebSocketServerProtocol:
    with mock.patch('websockets.server.WebSocketServerProtocol'):
        module = websockets.server
        websocket = module.WebSocketServerProtocol()  # type: ignore[call-arg]
        headers = websockets.datastructures.Headers()
        websocket.request_headers = headers
        return websocket


@pytest.mark.asyncio()
async def test_server_send() -> None:
    server = RelayServer(NullAuthenticator())
    message = RelayRegistrationRequest('name', uuid.uuid4())
    client = Client('name', uuid.uuid4(), NullUser(), get_mock_websocket())

    with mock.patch.object(
        client.websocket,
        'send',
        mock.AsyncMock(),
    ) as mock_send:
        await server.send(client, message)
        mock_send.assert_awaited_once()


@pytest.mark.asyncio()
async def test_server_send_encoding_error(caplog) -> None:
    caplog.set_level(logging.ERROR)
    server = RelayServer(NullAuthenticator())
    client = object()
    message = object()
    await server.send(client, message)  # type: ignore[arg-type]
    assert len(caplog.records) == 1
    assert 'Failed to encode message' in caplog.records[0].message


@pytest.mark.asyncio()
async def test_server_send_connection_closed(caplog) -> None:
    caplog.set_level(logging.ERROR)
    server = RelayServer(NullAuthenticator())
    message = RelayRegistrationRequest('name', uuid.uuid4())
    client = Client('name', uuid.uuid4(), NullUser(), get_mock_websocket())
    exception = websockets.exceptions.ConnectionClosedOK(None, None)

    with mock.patch.object(
        client.websocket,
        'send',
        mock.AsyncMock(side_effect=exception),
    ) as mock_send:
        await server.send(client, message)
        mock_send.assert_awaited_once()

    assert len(caplog.records) == 1
    assert 'Connection closed while' in caplog.records[0].message


@pytest.mark.asyncio()
async def test_server_register() -> None:
    server = RelayServer(NullAuthenticator())
    request = RelayRegistrationRequest('name', uuid.uuid4())
    websocket = get_mock_websocket()

    with mock.patch.object(websocket, 'send', AsyncMock()) as mock_send:
        await server.register(websocket, request)
        mock_send.assert_awaited_once()

    client = server.client_manager.get_client_by_uuid(request.uuid)
    assert client is not None
    assert client.name == 'name'
    assert client.uuid == request.uuid


@pytest.mark.asyncio()
async def test_server_register_override_same_websocket(caplog) -> None:
    caplog.set_level(logging.INFO)
    server = RelayServer(NullAuthenticator())
    client = Client('name', uuid.uuid4(), NullUser(), get_mock_websocket())
    server.client_manager.add_client(client)

    request = RelayRegistrationRequest(client.name, client.uuid)

    with mock.patch.object(
        server,
        'unregister',
    ) as mock_unregister, mock.patch.object(
        server,
        'send',
    ) as mock_send:
        await server.register(client.websocket, request)
        mock_send.assert_awaited_once()
        mock_unregister.assert_not_awaited()

    assert server.client_manager.get_client_by_uuid(request.uuid) is not None
    assert not any(
        [
            f'Previously registered client {request.uuid}' in record.message
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_server_register_override_different_websocket(caplog) -> None:
    caplog.set_level(logging.INFO)
    server = RelayServer(NullAuthenticator())
    client = Client('name', uuid.uuid4(), NullUser(), get_mock_websocket())
    server.client_manager.add_client(client)

    new_websocket = get_mock_websocket()
    request = RelayRegistrationRequest(client.name, client.uuid)

    with mock.patch.object(
        server,
        'unregister',
    ) as mock_unregister, mock.patch.object(
        server,
        'send',
    ) as mock_send:
        await server.register(new_websocket, request)
        mock_send.assert_awaited_once()
        mock_unregister.assert_awaited_once()

    assert server.client_manager.get_client_by_uuid(request.uuid) is not None
    assert any(
        [
            f'Previously registered client {request.uuid}' in record.message
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_server_register_with_different_users_uuid(caplog) -> None:
    caplog.set_level(logging.INFO)
    server = RelayServer(NullAuthenticator())
    client = Client('name', uuid.uuid4(), NullUser(), get_mock_websocket())
    server.client_manager.add_client(client)

    new_request = RelayRegistrationRequest('name', client.uuid)
    new_user = object()

    with mock.patch.object(
        server.authenticator,
        'authenticate_user',
        return_value=new_user,
    ), pytest.raises(
        ForbiddenError,
        match=f'The client UUID {new_request.uuid} is already registered',
    ):
        await server.register(client.websocket, new_request)


@pytest.mark.asyncio()
async def test_server_unregister(caplog) -> None:
    caplog.set_level(logging.INFO)
    server = RelayServer(NullAuthenticator())
    client = Client('name', uuid.uuid4(), NullUser(), get_mock_websocket())
    server.client_manager.add_client(client)

    with mock.patch.object(
        client.websocket,
        'close',
        mock.AsyncMock(),
    ) as mock_close:
        await server.unregister(client, expected=True)
        mock_close.assert_awaited_once()

    assert server.client_manager.get_client_by_uuid(client.uuid) is None
    assert any(
        [
            f'Unregistering client {client.uuid}' in record.message
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_forward_request_to_peer() -> None:
    server = RelayServer(NullAuthenticator())
    client = Client('client', uuid.uuid4(), NullUser(), get_mock_websocket())
    peer = Client('peer', uuid.uuid4(), NullUser(), get_mock_websocket())
    server.client_manager.add_client(client)
    server.client_manager.add_client(peer)

    request = PeerConnectionRequest(
        source_uuid=client.uuid,
        source_name=client.name,
        peer_uuid=peer.uuid,
        description_type='offer',
        description='description',
    )

    with mock.patch.object(
        peer.websocket,
        'send',
        mock.AsyncMock(),
    ) as mock_send:
        await server.forward(client, request)
        mock_send.assert_awaited_once()


@pytest.mark.asyncio()
async def test_forward_request_to_unknown_peer() -> None:
    server = RelayServer(NullAuthenticator())
    client = Client('client', uuid.uuid4(), NullUser(), get_mock_websocket())
    server.client_manager.add_client(client)

    request = PeerConnectionRequest(
        source_uuid=client.uuid,
        source_name=client.name,
        peer_uuid=uuid.uuid4(),
        description_type='offer',
        description='description',
    )

    async def _mock_send(
        # _server_self: Any,
        _client: Client[NullUser],
        message: RelayMessage,
    ) -> None:
        assert isinstance(message, PeerConnectionRequest)
        assert message.error is not None
        assert (
            'Cannot forward peer connection message to peer' in message.error
        )

    with mock.patch.object(server, 'send', AsyncMock(side_effect=_mock_send)):
        await server.forward(client, request)


@pytest.mark.asyncio()
async def test_forward_request_to_peer_owned_by_different_user() -> None:
    server = RelayServer(NullAuthenticator())
    client = Client('client', uuid.uuid4(), NullUser(), get_mock_websocket())
    # All NullUser instances are equal so patch in a new object()
    peer = Client('peer', uuid.uuid4(), object(), get_mock_websocket())
    server.client_manager.add_client(client)
    server.client_manager.add_client(peer)  # type: ignore[arg-type]

    request = PeerConnectionRequest(
        source_uuid=client.uuid,
        source_name=client.name,
        peer_uuid=peer.uuid,
        description_type='offer',
        description='description',
    )

    async def _mock_send(
        # _server_self: Any,
        _client: Client[NullUser],
        message: RelayMessage,
    ) -> None:
        assert isinstance(message, PeerConnectionRequest)
        assert message.error is not None
        assert (
            f'{request.peer_uuid} is owned by a different user'
            in message.error
        )

    with mock.patch.object(server, 'send', AsyncMock(side_effect=_mock_send)):
        await server.forward(client, request)


@pytest.mark.asyncio()
async def test_handler_register_and_connect(
    relay_server: RelayServerInfo,
) -> None:
    client_uuids = (uuid.uuid4(), uuid.uuid4())
    client_sockets = []

    for client_uuid in client_uuids:
        websocket = await websockets.client.connect(relay_server.address)
        request = RelayRegistrationRequest('name', client_uuid)
        await asyncio.wait_for(
            websocket.send(encode_relay_message(request)),
            _WAIT_FOR,
        )

        result_str = await asyncio.wait_for(websocket.recv(), _WAIT_FOR)
        assert isinstance(result_str, str)
        result = decode_relay_message(result_str)
        assert isinstance(result, RelayResponse)
        assert result.success

        client_sockets.append(websocket)

    peer_request = PeerConnectionRequest(
        source_uuid=client_uuids[0],
        source_name='name',
        peer_uuid=client_uuids[1],
        description_type='offer',
        description='description',
    )
    await asyncio.wait_for(
        client_sockets[0].send(encode_relay_message(peer_request)),
        _WAIT_FOR,
    )

    request_str = await asyncio.wait_for(client_sockets[1].recv(), _WAIT_FOR)
    assert isinstance(request_str, str)
    assert decode_relay_message(request_str) == peer_request

    client_manager = relay_server.relay_server.client_manager
    assert len(client_manager.get_clients()) >= 2

    # Both okay and error closures should unregister user
    await client_sockets[0].close(code=1000)
    await client_sockets[1].close(code=1002)

    # Yield control of event loop to allow server to process closure
    for _ in range(5):
        await asyncio.sleep(0)

    for client_uuid in client_uuids:
        assert client_manager.get_client_by_uuid(client_uuid) is None


@pytest.mark.asyncio()
async def test_handler_bad_message_type_closes_socket(
    relay_server: RelayServerInfo,
) -> None:
    websocket = await websockets.client.connect(relay_server.address)
    await asyncio.wait_for(websocket.send(b'message'), _WAIT_FOR)

    await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
    assert websocket.close_code == 4000
    assert websocket.close_reason == 'Unknown message type.'


@pytest.mark.asyncio()
async def test_handler_unauthorized_error(
    relay_server: RelayServerInfo,
) -> None:
    with mock.patch.object(
        relay_server.relay_server.authenticator,
        'authenticate_user',
        side_effect=UnauthorizedError('Test unauthorized error.'),
    ):
        async with websockets.client.connect(
            relay_server.address,
        ) as websocket:
            request = RelayRegistrationRequest('name', uuid.uuid4())
            await asyncio.wait_for(
                websocket.send(encode_relay_message(request)),
                _WAIT_FOR,
            )

            await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
            assert websocket.close_code == 4001
            assert websocket.close_reason == (
                'UnauthorizedError: Test unauthorized error.'
            )


@pytest.mark.asyncio()
async def test_handler_forbidden_error(
    relay_server: RelayServerInfo,
) -> None:
    with mock.patch.object(
        relay_server.relay_server.authenticator,
        'authenticate_user',
        side_effect=ForbiddenError('Test forbidden error.'),
    ):
        async with websockets.client.connect(
            relay_server.address,
        ) as websocket:
            request = RelayRegistrationRequest('name', uuid.uuid4())
            await asyncio.wait_for(
                websocket.send(encode_relay_message(request)),
                _WAIT_FOR,
            )

            await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
            assert websocket.close_code == 4002
            assert (
                websocket.close_reason
                == 'ForbiddenError: Test forbidden error.'
            )


@pytest.mark.asyncio()
async def test_handler_bad_request_error(
    relay_server: RelayServerInfo,
) -> None:
    with mock.patch.object(
        relay_server.relay_server.authenticator,
        'authenticate_user',
        side_effect=BadRequestError('Test bad request error.'),
    ):
        async with websockets.client.connect(
            relay_server.address,
        ) as websocket:
            request = RelayRegistrationRequest('name', uuid.uuid4())
            await asyncio.wait_for(
                websocket.send(encode_relay_message(request)),
                _WAIT_FOR,
            )

            response_str = await asyncio.wait_for(websocket.recv(), _WAIT_FOR)
            assert isinstance(response_str, str)
            response = decode_relay_message(response_str)
            assert isinstance(response, RelayResponse)
            assert not response.success
            assert response.error
            assert response.message == (
                'BadRequestError: Test bad request error.'
            )


@pytest.mark.asyncio()
async def test_handler_message_size_exceeded(
    relay_server: RelayServerInfo,
) -> None:
    max_size = 1000
    with mock.patch.object(
        relay_server.relay_server,
        '_max_message_bytes',
        max_size,
    ):
        async with websockets.client.connect(
            relay_server.address,
        ) as websocket:
            request = PeerConnectionRequest(
                source_uuid=uuid.uuid4(),
                source_name='test',
                peer_uuid=uuid.uuid4(),
                description_type='offer',
                description='.' * max_size,
            )
            await asyncio.wait_for(
                websocket.send(encode_relay_message(request)),
                _WAIT_FOR,
            )

            await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
            assert websocket.close_code == 4003
            assert websocket.close_reason == 'Message length exceeds limit.'


@pytest.mark.asyncio()
async def test_handler_forward_request_before_registration(
    relay_server: RelayServerInfo,
) -> None:
    async with websockets.client.connect(
        relay_server.address,
    ) as websocket:
        request = PeerConnectionRequest(
            source_uuid=uuid.uuid4(),
            source_name='name',
            peer_uuid=uuid.uuid4(),
            description_type='offer',
            description='description',
        )
        await asyncio.wait_for(
            websocket.send(encode_relay_message(request)),
            _WAIT_FOR,
        )

        await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
        assert websocket.close_code == 4002
        assert websocket.close_reason == (
            'ForbiddenError: Client has not registered and authenticated '
            'with the relay server.'
        )

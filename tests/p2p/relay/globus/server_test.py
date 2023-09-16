from __future__ import annotations

import asyncio
import logging
import ssl
import uuid
from unittest import mock

import pytest
import websockets

from proxystore.globus.client import get_confidential_app_auth_client
from proxystore.p2p.relay.exceptions import BadRequestError
from proxystore.p2p.relay.exceptions import ForbiddenError
from proxystore.p2p.relay.exceptions import UnauthorizedError
from proxystore.p2p.relay.globus.manager import Client
from proxystore.p2p.relay.globus.server import GlobusAuthRelayServer
from proxystore.p2p.relay.globus.utils import GlobusUser
from proxystore.p2p.relay.messages import decode_relay_message
from proxystore.p2p.relay.messages import encode_relay_message
from proxystore.p2p.relay.messages import PeerConnectionRequest
from proxystore.p2p.relay.messages import RelayRegistrationRequest
from proxystore.p2p.relay.messages import RelayResponse
from tests.p2p.relay.globus.conftest import RelayServerInfo

_WAIT_FOR = 0.2


async def connect(address: str) -> websockets.client.WebSocketClientProtocol:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    return await websockets.client.connect(
        address,
        ssl=ssl_context,
        extra_headers={'Authorization': 'Bearer <TOKEN>'},
    )


def get_mock_websocket() -> websockets.server.WebSocketServerProtocol:
    with mock.patch('websockets.server.WebSocketServerProtocol'):
        websocket = websockets.server.WebSocketServerProtocol()  # type: ignore[call-arg]  # noqa: E501
        headers = websockets.datastructures.Headers(
            **{'Authorization': 'Bearer <TOKEN>'},
        )
        websocket.request_headers = headers
        return websocket


def generate_client(globus_user: GlobusUser | None = None) -> Client:
    if globus_user is None:
        globus_user = GlobusUser('username', uuid.uuid4())
    return Client('client', uuid.uuid4(), globus_user, get_mock_websocket())


@pytest.fixture()
def server() -> GlobusAuthRelayServer:
    auth_client = get_confidential_app_auth_client(
        str(uuid.uuid4()),
        '<SECRET>',
    )
    return GlobusAuthRelayServer(auth_client)


@pytest.fixture()
def mock_websocket() -> websockets.server.WebSocketServerProtocol:
    return get_mock_websocket()


@pytest.mark.asyncio()
async def test_server_send(
    server: GlobusAuthRelayServer,
    mock_websocket: websockets.server.WebSocketServerProtocol,
) -> None:
    message = RelayRegistrationRequest('name', uuid.uuid4())

    with mock.patch.object(
        mock_websocket,
        'send',
        mock.AsyncMock(),
    ) as mock_send:
        await server.send(mock_websocket, message)
        mock_send.assert_awaited_once()


@pytest.mark.asyncio()
async def test_server_send_encoding_error(
    server: GlobusAuthRelayServer,
    mock_websocket: websockets.server.WebSocketServerProtocol,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    message = object()
    await server.send(mock_websocket, message)  # type: ignore[arg-type]
    assert len(caplog.records) == 1
    assert 'Failed to encode message' in caplog.records[0].message


@pytest.mark.asyncio()
async def test_server_send_connection_closed(
    server: GlobusAuthRelayServer,
    mock_websocket: websockets.server.WebSocketServerProtocol,
    caplog,
) -> None:
    caplog.set_level(logging.ERROR)
    message = RelayRegistrationRequest('name', uuid.uuid4())
    exception = websockets.exceptions.ConnectionClosedOK(None, None)

    with mock.patch.object(
        mock_websocket,
        'send',
        mock.AsyncMock(side_effect=exception),
    ) as mock_send:
        await server.send(mock_websocket, message)
        mock_send.assert_awaited_once()

    assert len(caplog.records) == 1
    assert 'Connection closed while' in caplog.records[0].message


@pytest.mark.asyncio()
async def test_server_register(
    server: GlobusAuthRelayServer,
    mock_websocket: websockets.server.WebSocketServerProtocol,
) -> None:
    message = RelayRegistrationRequest('name', uuid.uuid4())
    globus_user = GlobusUser(username='username', client_id=uuid.uuid4())

    with mock.patch(
        'proxystore.p2p.relay.globus.server.authenticate_user_with_token',
        return_value=globus_user,
    ), mock.patch.object(server, 'send') as mock_send:
        await server.register(mock_websocket, message)
        mock_send.assert_awaited_once()

    client = server.client_manager.get_client_by_uuid(message.uuid)
    assert client is not None


@pytest.mark.asyncio()
async def test_server_register_override_existing(
    server: GlobusAuthRelayServer,
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    old_client = generate_client()
    message = RelayRegistrationRequest(old_client.name, old_client.uuid)
    server.client_manager.add_client(old_client)

    with mock.patch(
        'proxystore.p2p.relay.globus.server.authenticate_user_with_token',
        return_value=old_client.globus_user,
    ), mock.patch.object(
        server,
        'unregister',
    ) as mock_unregister, mock.patch.object(
        server,
        'send',
    ) as mock_send:
        await server.register(old_client.websocket, message)
        mock_send.assert_awaited_once()
        mock_unregister.assert_awaited_once()

    client = server.client_manager.get_client_by_uuid(message.uuid)
    assert client is not None

    assert any(
        [
            'old registration will be removed' in record.message
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_server_register_with_different_users_uuid(
    server: GlobusAuthRelayServer,
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    existing_client = generate_client()
    server.client_manager.add_client(existing_client)

    message = RelayRegistrationRequest('name', existing_client.uuid)
    new_globus_user = GlobusUser(username='username', client_id=uuid.uuid4())

    with mock.patch(
        'proxystore.p2p.relay.globus.server.authenticate_user_with_token',
        return_value=new_globus_user,
    ), pytest.raises(
        ForbiddenError,
        match=f'The client UUID {message.uuid} is already registered',
    ):
        await server.register(existing_client.websocket, message)


@pytest.mark.asyncio()
async def test_server_unregister(
    server: GlobusAuthRelayServer,
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    existing_client = generate_client()
    server.client_manager.add_client(existing_client)

    with mock.patch.object(
        existing_client.websocket,
        'close',
        mock.AsyncMock(),
    ) as mock_close:
        await server.unregister(existing_client.websocket, expected=True)
        mock_close.assert_awaited_once()

    client = server.client_manager.get_client_by_uuid(
        existing_client.uuid,
    )
    assert client is None

    assert any(
        [
            f'Unregistering client {existing_client.uuid}' in record.message
            for record in caplog.records
        ],
    )


@pytest.mark.asyncio()
async def test_server_unregister_unknown_client(
    server: GlobusAuthRelayServer,
    mock_websocket: websockets.server.WebSocketServerProtocol,
) -> None:
    # Should be a no-op
    await server.unregister(mock_websocket, expected=True)
    await server.unregister(mock_websocket, expected=True)


@pytest.mark.asyncio()
async def test_server_connect_to_peer(
    server: GlobusAuthRelayServer,
) -> None:
    globus_user = GlobusUser(username='username', client_id=uuid.uuid4())
    client = generate_client(globus_user)
    peer = generate_client(globus_user)
    server.client_manager.add_client(client)
    server.client_manager.add_client(peer)

    message = PeerConnectionRequest(
        source_uuid=client.uuid,
        source_name=client.name,
        peer_uuid=peer.uuid,
        description_type='offer',
        description='description',
    )

    with mock.patch.object(server, 'send', mock.AsyncMock()) as mock_send:
        await server.connect(client.websocket, message)
        mock_send.assert_awaited_once()


@pytest.mark.asyncio()
async def test_server_connect_to_peer_client_unauthenticated(
    server: GlobusAuthRelayServer,
    mock_websocket: websockets.server.WebSocketServerProtocol,
) -> None:
    message = PeerConnectionRequest(
        source_uuid=uuid.uuid4(),
        source_name='name',
        peer_uuid=uuid.uuid4(),
        description_type='offer',
        description='description',
    )

    with pytest.raises(
        ForbiddenError,
        match='Client has not registered and authenticated with the relay',
    ):
        await server.connect(mock_websocket, message)


@pytest.mark.asyncio()
async def test_server_connect_to_peer_unknown_peer(
    server: GlobusAuthRelayServer,
) -> None:
    client = generate_client()
    server.client_manager.add_client(client)

    message = PeerConnectionRequest(
        source_uuid=uuid.uuid4(),
        source_name='name',
        peer_uuid=uuid.uuid4(),
        description_type='offer',
        description='description',
    )

    with pytest.raises(
        BadRequestError,
        match=(
            'Cannot forward peer connection message to peer '
            f'{message.peer_uuid} because this peer is not registered'
        ),
    ):
        await server.connect(client.websocket, message)


@pytest.mark.asyncio()
async def test_server_connect_to_peer_owned_by_different_user(
    server: GlobusAuthRelayServer,
) -> None:
    client = generate_client()
    peer = generate_client()
    server.client_manager.add_client(client)
    server.client_manager.add_client(peer)

    message = PeerConnectionRequest(
        source_uuid=client.uuid,
        source_name=client.name,
        peer_uuid=peer.uuid,
        description_type='offer',
        description='description',
    )

    with pytest.raises(
        ForbiddenError,
        match=(
            f'The requested peer {message.peer_uuid} is owned by a '
            'different user.'
        ),
    ):
        await server.connect(client.websocket, message)


@pytest.mark.asyncio()
async def test_handler_register_and_connect(
    globus_auth_relay: RelayServerInfo,
) -> None:
    client1 = generate_client()
    client2 = generate_client(client1.globus_user)
    client_sockets = []

    globus_auth_relay.mock_authenticate_user.return_value = client1.globus_user

    for client in (client1, client2):
        websocket = await connect(globus_auth_relay.address)
        request = RelayRegistrationRequest(client.name, client.uuid)
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

    message = PeerConnectionRequest(
        source_uuid=client1.uuid,
        source_name=client1.name,
        peer_uuid=client2.uuid,
        description_type='offer',
        description='description',
    )
    await asyncio.wait_for(
        client_sockets[0].send(encode_relay_message(message)),
        _WAIT_FOR,
    )

    request_str = await asyncio.wait_for(client_sockets[1].recv(), _WAIT_FOR)
    assert isinstance(request_str, str)
    peer_request = decode_relay_message(request_str)
    assert peer_request == message

    client_manager = globus_auth_relay.relay_server.client_manager
    assert len(client_manager.get_clients()) == 2

    # Both okay and error closures should unregister user
    await client_sockets[0].close(code=1000)
    await client_sockets[1].close(code=1002)

    # Yield control of event loop to allow server to process closure
    for _ in range(5):
        await asyncio.sleep(0)

    assert len(client_manager.get_clients()) == 0


@pytest.mark.asyncio()
async def test_handler_bad_message_type_closes_socket(
    globus_auth_relay: RelayServerInfo,
) -> None:
    websocket = await connect(globus_auth_relay.address)
    await asyncio.wait_for(websocket.send('message'), _WAIT_FOR)

    await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
    assert websocket.close_code == 4000
    assert websocket.close_reason == 'Unknown message type.'


@pytest.mark.asyncio()
async def test_handler_unauthorized_error(
    globus_auth_relay: RelayServerInfo,
) -> None:
    globus_auth_relay.mock_authenticate_user.side_effect = UnauthorizedError(
        'Test unauthorized error.',
    )

    websocket = await connect(globus_auth_relay.address)
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
    globus_auth_relay: RelayServerInfo,
) -> None:
    globus_auth_relay.mock_authenticate_user.side_effect = ForbiddenError(
        'Test forbidden error.',
    )

    websocket = await connect(globus_auth_relay.address)
    request = RelayRegistrationRequest('name', uuid.uuid4())
    await asyncio.wait_for(
        websocket.send(encode_relay_message(request)),
        _WAIT_FOR,
    )

    await asyncio.wait_for(websocket.wait_closed(), _WAIT_FOR)
    assert websocket.close_code == 4002
    assert websocket.close_reason == 'ForbiddenError: Test forbidden error.'


@pytest.mark.asyncio()
async def test_handler_bad_request_error(
    globus_auth_relay: RelayServerInfo,
) -> None:
    globus_auth_relay.mock_authenticate_user.side_effect = BadRequestError(
        'Test bad request error.',
    )

    websocket = await connect(globus_auth_relay.address)
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
    assert response.message == 'BadRequestError: Test bad request error.'

    await websocket.close()

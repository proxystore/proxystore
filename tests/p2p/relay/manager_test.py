from __future__ import annotations

import uuid
from unittest import mock

import websockets

from proxystore.p2p.relay.authenticate import GlobusUser
from proxystore.p2p.relay.manager import Client
from proxystore.p2p.relay.manager import ClientManager


def mock_websocket() -> websockets.server.WebSocketServerProtocol:
    with mock.patch('websockets.server.WebSocketServerProtocol'):
        return websockets.server.WebSocketServerProtocol()  # type: ignore[call-arg]


def generate_client() -> Client[GlobusUser]:
    return Client(
        name='name',
        uuid=uuid.uuid4(),
        user=GlobusUser('username', uuid.uuid4()),
        websocket=mock_websocket(),
    )


def test_client_equality() -> None:
    assert generate_client() != generate_client()

    client1 = generate_client()
    client2 = Client(
        name='other-name',
        uuid=client1.uuid,
        user=client1.user,
        websocket=mock_websocket(),
    )
    assert client1 == client2

    assert client1 != object


def test_client_repr() -> None:
    assert isinstance(repr(generate_client()), str)


def test_client_manager() -> None:
    manager: ClientManager[GlobusUser] = ClientManager()

    # Test operations on empty manager
    assert len(manager.get_clients()) == 0
    assert manager.get_client_by_uuid(uuid.uuid4()) is None
    assert manager.get_client_by_websocket(mock_websocket()) is None

    # Basic add / get client
    client = generate_client()
    manager.add_client(client)
    assert len(manager.get_clients()) == 1
    assert manager.get_client_by_uuid(client.uuid) is client
    assert manager.get_client_by_websocket(client.websocket) is client

    # Remove a client + remove an already removed client
    manager.remove_client(client)
    assert len(manager.get_clients()) == 0
    manager.remove_client(client)

    # Add many clients
    count = 5
    for _ in range(count):
        manager.add_client(generate_client())
    assert len(manager.get_clients()) == count

"""Helper classes for managing clients connected to the relay server."""
from __future__ import annotations

import dataclasses
import datetime
import uuid

try:
    from websockets.server import WebSocketServerProtocol
except ImportError as e:  # pragma: no cover
    import warnings

    warnings.warn(
        f'{e}. To enable endpoint serving, install proxystore with '
        '"pip install proxystore[endpoints]".',
        stacklevel=2,
    )

from proxystore.p2p.relay.globus.utils import GlobusUser


def _utc_current_time() -> datetime.datetime:
    # dataclasses.field's default_factory requires a zero argument callable
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclasses.dataclass(frozen=True, eq=False)
class Client:
    """Representation of client connection.

    Attributes:
        name: Name of client.
        uuid: UUID of client.
        globus_user: Globus Auth user information.
        websocket: WebSocket connection to the client.
        created: Time the client was created at.
    """

    name: str
    uuid: uuid.UUID
    globus_user: GlobusUser
    websocket: WebSocketServerProtocol
    created: datetime.datetime = dataclasses.field(
        default_factory=_utc_current_time,
    )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Client):
            return (self.uuid == other.uuid) and (
                self.globus_user == other.globus_user
            )
        else:
            return False

    def __repr__(self) -> str:
        created = self.created.strftime('%Y-%m-%d %H:%M:%S %Z')
        return (
            f'{self.__class__.__name__}(name={self.name}, uuid={self.uuid}, '
            f'address={self.websocket.remote_address}, created={created})'
        )


class ClientManager:
    """Manages active connections to authenticated clients.

    This is intended for internal use by the
    [`GlobusAuthRelayServer`][proxystore.p2p.relay.globus.server.GlobusAuthRelayServer].
    """

    def __init__(self) -> None:
        self._clients_by_uuid: dict[uuid.UUID, Client] = {}
        self._clients_by_websocket: dict[WebSocketServerProtocol, Client] = {}

    def add_client(self, client: Client) -> None:
        """Add a new authenticated client."""
        self._clients_by_uuid[client.uuid] = client
        self._clients_by_websocket[client.websocket] = client

    def get_clients(self) -> list[Client]:
        """Get a list of all clients."""
        return list(self._clients_by_uuid.values())

    def get_client_by_uuid(self, uuid: uuid.UUID) -> Client | None:
        """Get a client by the client's UUID."""
        return self._clients_by_uuid.get(uuid, None)

    def get_client_by_websocket(
        self,
        websocket: WebSocketServerProtocol,
    ) -> Client | None:
        """Get a client by the current websocket connection."""
        return self._clients_by_websocket.get(websocket, None)

    def remove_client(self, client: Client) -> None:
        """Remove a client."""
        self._clients_by_uuid.pop(client.uuid, None)
        self._clients_by_websocket.pop(client.websocket, None)

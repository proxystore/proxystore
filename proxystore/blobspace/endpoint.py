"""ProxyStore BlobSpace Endpoint."""
from __future__ import annotations

from typing import Any
from typing import Generator

from proxystore.blobspace.p2p import P2PConnectionManager
from proxystore.blobspace.server import connect


class Endpoint:
    """BlobSpace Endpoint.

    Endpoints act as distributed blob stores. Endpoints support peer-to-peer
    communication for retrieving data not located on the local endpoint.
    """

    def __init__(
        self,
        uuid: str,
        name: str,
        signaling_server_address: str,
    ) -> None:
        """Init Endpoint.

        Args:
            uuid (str): uuid of endpoint.
            name (str): readable name of endpoint.
            signaling_server_address (str): address of signaling server
                used for peer-to-peer connections between endpoints.
        """
        self._uuid = uuid
        self._name = name
        self._signaling_server_address = signaling_server_address

    def __await__(self) -> Generator[Any, None, Endpoint]:
        """Initialize Endpoint awaitables."""
        return self._async_init().__await__()

    async def _async_init(self) -> Endpoint:
        self._signaling_server_socket = await connect(
            uuid=self._uuid,
            name=self._name,
            address=self._signaling_server_address,
        )
        self._p2p_manager = P2PConnectionManager(
            self._uuid,
            self._name,
            self._signaling_server_socket,
        )
        return self

    async def _connect_to_peer(self, target_uuid: str) -> None:
        await self._p2p_manager.new_connection(target_uuid)

    async def close(self) -> None:
        """Close the endpoint and any open connections safely."""
        await self._p2p_manager.close()
        await self._signaling_server_socket.close()

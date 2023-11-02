"""Endpoint connector implementation."""
from __future__ import annotations

import logging
import sys
import uuid
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence
from uuid import UUID

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import requests

from proxystore.endpoint import client
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.utils.environment import home_dir

logger = logging.getLogger(__name__)


class EndpointConnectorError(Exception):
    """Exception resulting from request to Endpoint."""

    pass


class EndpointKey(NamedTuple):
    """Key to object in an Endpoint.

    Attributes:
        object_id: Unique object ID.
        endpoint_id: Endpoint UUID where object is stored.
    """

    object_id: str
    endpoint_id: str | None


class EndpointConnector:
    """Connector to ProxyStore Endpoints.

    Warning:
        Specifying a custom `proxystore_dir` can cause problems if the
        `proxystore_dir` is not the same on all systems that a proxy
        created by this store could end up on. It is recommended to leave
        the `proxystore_dir` unspecified so the correct default directory
        will be used.

    Args:
        endpoints: Sequence of valid and running endpoint
            UUIDs to use. At least one of these endpoints must be
            accessible by this process.
        proxystore_dir: Optionally specify the proxystore home
            directory. Defaults to
            [`home_dir()`][proxystore.utils.environment.home_dir].

    Raises:
        ValueError: If endpoints is an empty list.
        EndpointConnectorError: If unable to connect to one of the endpoints
            provided.
    """

    def __init__(
        self,
        endpoints: Sequence[str | UUID],
        proxystore_dir: str | None = None,
    ) -> None:
        if len(endpoints) == 0:
            raise ValueError('At least one endpoint must be specified.')
        self.endpoints: list[UUID] = [
            e if isinstance(e, UUID) else UUID(e, version=4) for e in endpoints
        ]
        self.proxystore_dir = proxystore_dir

        # Maintain single session for connection pooling persistence to
        # speed up repeat requests to same endpoint.
        self._session = requests.Session()

        # Find the first locally accessible endpoint to use as our
        # home endpoint
        available_endpoints = get_configs(
            home_dir() if self.proxystore_dir is None else self.proxystore_dir,
        )
        found_endpoint: EndpointConfig | None = None
        for endpoint in available_endpoints:
            endpoint_uuid = UUID(endpoint.uuid)
            if endpoint_uuid in self.endpoints:
                logger.debug(f'Attempting connection to {endpoint_uuid}')
                response = self._session.get(
                    f'http://{endpoint.host}:{endpoint.port}/endpoint',
                )
                if response.status_code == 200:
                    uuid_ = response.json()['uuid']
                    if endpoint_uuid == UUID(uuid_):
                        logger.debug(
                            f'Connection to {endpoint_uuid} successful, using '
                            'as local endpoint',
                        )
                        found_endpoint = endpoint
                        break
                    else:
                        logger.debug(
                            f'Connection to {endpoint_uuid} returned '
                            'different UUID',
                        )
                else:
                    logger.debug(f'Connection to {endpoint_uuid} failed')

        if found_endpoint is None:
            self._session.close()
            raise EndpointConnectorError(
                'Failed to find an endpoint configuration matching one of the '
                'provided endpoint UUIDs, or an endpoint configuration was '
                'found but the endpoint could not be connected to. '
                'Enable debug level logging for more more details.',
            )
        self.endpoint_uuid: uuid.UUID = uuid.UUID(found_endpoint.uuid)
        self.endpoint_host: str | None = found_endpoint.host
        self.endpoint_port: int = found_endpoint.port

        self.address = f'http://{self.endpoint_host}:{self.endpoint_port}'

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(connected to {self.endpoint_uuid} '
            f'@ {self.address})'
        )

    def close(self) -> None:
        """Close the connector and clean up."""
        self._session.close()

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'endpoints': [str(ep) for ep in self.endpoints],
            'proxystore_dir': self.proxystore_dir,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> EndpointConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: EndpointKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        try:
            client.evict(
                self.address,
                key.object_id,
                key.endpoint_id,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise EndpointConnectorError(
                f'Evict failed with error code {e.response.status_code}.',
            ) from e

    def exists(self, key: EndpointKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        try:
            return client.exists(
                self.address,
                key.object_id,
                key.endpoint_id,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise EndpointConnectorError(
                f'Exists failed with error code {e.response.status_code}.',
            ) from e

    def get(self, key: EndpointKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        try:
            return client.get(
                self.address,
                key.object_id,
                key.endpoint_id,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise EndpointConnectorError(
                f'Get failed with error code {e.response.status_code}.',
            ) from e

    def get_batch(self, keys: Sequence[EndpointKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def new_key(self, obj: bytes | None = None) -> EndpointKey:
        """Create a new key.

        Warning:
            The returned key will be associated with this instance's local
            endpoint. I.e., when
            [`set()`][proxystore.connectors.endpoint.EndpointConnector.set]
            is called on this key, the connector must be connected to the same
            local endpoint.

        Args:
            obj: Optional object which the key will be associated with.
                Ignored in this implementation.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.endpoint.EndpointConnector.set] \
            has been called on the key.
        """
        return EndpointKey(
            object_id=str(uuid.uuid4()),
            endpoint_id=str(self.endpoint_uuid),
        )

    def put(self, obj: bytes) -> EndpointKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = EndpointKey(
            object_id=str(uuid.uuid4()),
            endpoint_id=str(self.endpoint_uuid),
        )
        self.set(key, obj)
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[EndpointKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]

    def set(self, key: EndpointKey, obj: bytes) -> None:
        """Set the object associated with a key.

        Note:
            The [`Connector`][proxystore.connectors.protocols.Connector]
            provides write-once, read-many semantics. Thus,
            [`set()`][proxystore.connectors.endpoint.EndpointConnector.set]
            should only be called once per key, otherwise unexpected behavior
            can occur.

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        try:
            client.put(
                self.address,
                key.object_id,
                obj,
                key.endpoint_id,
                session=self._session,
            )
        except requests.exceptions.RequestException as e:
            assert e.response is not None
            raise EndpointConnectorError(
                f'Put failed with error code {e.response.status_code}.',
            ) from e

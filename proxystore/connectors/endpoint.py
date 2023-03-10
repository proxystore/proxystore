"""Endpoint connector implementation."""
from __future__ import annotations

import logging
import uuid
from typing import Any
from typing import NamedTuple
from typing import Sequence
from uuid import UUID

import requests

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.constants import MAX_CHUNK_LENGTH
from proxystore.utils import chunk_bytes
from proxystore.utils import home_dir

logger = logging.getLogger(__name__)


class EndpointConnectorError(Exception):
    """Exception resulting from request to Endpoint."""

    pass


class EndpointKey(NamedTuple):
    """Key to object in an Endpoint."""

    object_id: str
    """Unique object ID."""
    endpoint_id: str | None
    """Endpoint UUID where object is stored."""


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
            directory. Defaults to [`home_dir()`][proxystore.utils.home_dir].

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

        # Find the first locally accessible endpoint to use as our
        # home endpoint
        available_endpoints = get_configs(
            home_dir() if self.proxystore_dir is None else self.proxystore_dir,
        )
        found_endpoint: EndpointConfig | None = None
        for endpoint in available_endpoints:
            if endpoint.uuid in self.endpoints:
                logger.debug(f'attempting connection to {endpoint.uuid}')
                response = requests.get(
                    f'http://{endpoint.host}:{endpoint.port}/endpoint',
                )
                if response.status_code == 200:
                    uuid = response.json()['uuid']
                    if str(endpoint.uuid) == uuid:
                        logger.debug(
                            f'connection to {endpoint.uuid} successful, using '
                            'as home endpoint',
                        )
                        found_endpoint = endpoint
                        break
                    else:
                        logger.debug(f'{endpoint.uuid} has different UUID')
                else:
                    logger.debug(f'connection to {endpoint.uuid} unsuccessful')

        if found_endpoint is None:
            raise EndpointConnectorError(
                'Failed to find endpoint configuration matching one of the '
                'provided endpoint UUIDs.',
            )
        self.endpoint_uuid = found_endpoint.uuid
        self.endpoint_host = found_endpoint.host
        self.endpoint_port = found_endpoint.port

        self.address = f'http://{self.endpoint_host}:{self.endpoint_port}'

    def close(self) -> None:
        """Close the connector and clean up."""
        pass

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
        response = requests.post(
            f'{self.address}/evict',
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
        )
        if response.status_code != 200:
            raise EndpointConnectorError(f'EVICT returned {response}')

    def exists(self, key: EndpointKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        response = requests.get(
            f'{self.address}/exists',
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
        )
        if response.status_code == 200:
            return response.json()['exists']
        else:
            raise EndpointConnectorError(f'EXISTS returned {response}')

    def get(self, key: EndpointKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        response = requests.get(
            f'{self.address}/get',
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
            stream=True,
        )
        if response.status_code == 200:
            data = bytearray()
            for chunk in response.iter_content(chunk_size=None):
                data += chunk
            return bytes(data)
        elif response.status_code == 400:
            return None
        else:
            raise EndpointConnectorError(f'GET returned {response}')

    def get_batch(self, keys: Sequence[EndpointKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

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

        response = requests.post(
            f'{self.address}/set',
            headers={'Content-Type': 'application/octet-stream'},
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
            data=chunk_bytes(obj, MAX_CHUNK_LENGTH),
            stream=True,
        )
        if response.status_code != 200:
            raise EndpointConnectorError(f'SET returned {response}')

        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[EndpointKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]
"""EndpointStore Implementation."""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

import requests

import proxystore as ps
from proxystore.endpoint.config import default_dir
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.serve import MAX_CHUNK_LENGTH
from proxystore.store.base import Store
from proxystore.utils import chunk_bytes

logger = logging.getLogger(__name__)


class EndpointStoreError(Exception):
    """Exception resulting from request to Endpoint."""

    pass


class EndpointStore(Store):
    """EndpointStore backend class."""

    def __init__(
        self,
        name: str,
        *,
        endpoints: list[str | UUID],
        proxystore_dir: str | None = None,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init EndpointStore.

        Warning:
            Specifying a custom `proxystore_dir` can cause problems if the
            `proxystore_dir` is not the same on all systems that a proxy
            created by this store could end up on. It is recommended to leave
            the `proxystore_dir` unspecified so the correct default directory
            will be used.

        Args:
            name (str): name of the store instance (default: None).
            endpoints (list): list of valid and running endpoint UUIDs to use.
                At least one of these endpoints must be accessible by this
                process.
            proxystore_dir (str): directory containing endpoint configurations.
                If None, defaults to :code:`$HOME/.proxystore` (default: None).
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).

        Raises:
            ValueError:
                if endpoints is an empty list.
            EndpointStoreError:
                if unable to connect to one of the endpoints provided.
        """
        if len(endpoints) == 0:
            raise ValueError('At least one endpoint must be specified.')
        self.endpoints: list[UUID] = [
            e if isinstance(e, UUID) else UUID(e, version=4) for e in endpoints
        ]
        self.proxystore_dir = (
            default_dir() if proxystore_dir is None else proxystore_dir
        )

        # Find the first locally accessible endpoint to use as our
        # home endpoint
        available_endpoints = get_configs(self.proxystore_dir)
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
            raise EndpointStoreError(
                'Failed to find endpoint configuration matching one of the '
                'provided endpoint UUIDs.',
            )
        self.endpoint_uuid = found_endpoint.uuid
        self.endpoint_host = found_endpoint.host
        self.endpoint_port = found_endpoint.port

        self.address = f'http://{self.endpoint_host}:{self.endpoint_port}'

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={
                'endpoints': self.endpoints,
                # Note: don't pass self.proxystore_dir here because it may
                # change depending on the system we are on (as a proxy could
                # reinitialize this store on a different system).
                'proxystore_dir': proxystore_dir,
            },
        )

    @staticmethod
    def _create_key(object_key: str, endpoint_uuid: UUID) -> str:
        return f'{object_key}:{str(endpoint_uuid)}'

    @staticmethod
    def _parse_key(key: str) -> tuple[str, UUID | None]:
        # TODO: validate format?
        values = key.split(':')
        if len(values) == 1:
            return values[0], None
        elif len(values) == 2:
            return (values[0], UUID(values[1], version=4))
        else:
            raise ValueError(f'Failed to parse key {key}.')

    def evict(self, key: str) -> None:
        """Evict object associated with key.

        Args:
            key (str): key corresponding to object in store to evict.

        Raises:
            EndpointStoreError:
                if the Endpoint returns a non-200 status code.
        """
        object_key, endpoint_uuid = self._parse_key(key)
        response = requests.post(
            f'{self.address}/evict',
            params={
                'key': object_key,
                'endpoint': str(endpoint_uuid)
                if endpoint_uuid is not None
                else None,
            },
        )
        if response.status_code != 200:
            raise EndpointStoreError(f'EVICT returned {response}')

        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        """Check if key exists.

        Args:
            key (str): key to check.

        Raises:
            EndpointStoreError:
                if the Endpoint returns a non-200 status code.
        """
        object_key, endpoint_uuid = self._parse_key(key)
        response = requests.get(
            f'{self.address}/exists',
            params={
                'key': object_key,
                'endpoint': str(endpoint_uuid)
                if endpoint_uuid is not None
                else None,
            },
        )
        if response.status_code == 200:
            return response.json()['exists']
        else:
            raise EndpointStoreError(f'EXISTS returned {response}')

    def get_bytes(self, key: str) -> bytes | None:
        """Get serialized object from remote store.

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist on the endpoint.

        Raises:
            EndpointStoreError:
                if the Endpoint returns a status code other than 200 (success)
                or 400 (missing key).
        """
        object_key, endpoint_uuid = self._parse_key(key)
        response = requests.get(
            f'{self.address}/get',
            params={
                'key': object_key,
                'endpoint': str(endpoint_uuid)
                if endpoint_uuid is not None
                else None,
            },
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
            raise EndpointStoreError(f'GET returned {response}')

    def get_timestamp(self, key: str) -> float:
        if not self.exists(key):
            raise KeyError(f'key={key} does not exists in the endpoint')
        return 0.0

    def set(
        self,
        obj: Any,
        *,
        key: str | None = None,
        serialize: bool = True,
    ) -> str:
        if serialize:
            obj = ps.serialize.serialize(obj)
        if not isinstance(obj, bytes):
            raise TypeError('obj must be of type bytes if serialize=False.')
        if key is None:
            key = self.create_key(obj)
        key = self._create_key(key, self.endpoint_uuid)

        self.set_bytes(key, obj)
        logger.debug(
            f"SET key='{key}' IN {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        return key

    def set_bytes(self, key: str, data: bytes) -> None:
        """Set serialized object in remote store with key.

        Args:
            key (str): key corresponding to object.
            data (bytes): serialized object.

        Raises:
            EndpointStoreError:
                if the endpoint does not return a 200 status code for success.
        """
        object_key, endpoint_uuid = self._parse_key(key)
        response = requests.post(
            f'{self.address}/set',
            headers={'Content-Type': 'application/octet-stream'},
            params={
                'key': object_key,
                'endpoint': str(endpoint_uuid)
                if endpoint_uuid is not None
                else None,
            },
            data=chunk_bytes(data, MAX_CHUNK_LENGTH),  # type: ignore
            stream=True,
        )
        if response.status_code != 200:
            raise EndpointStoreError(f'SET returned {response}')

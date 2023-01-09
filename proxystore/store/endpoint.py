"""EndpointStore Implementation."""
from __future__ import annotations

import logging
from typing import Any
from typing import NamedTuple
from typing import Sequence
from uuid import UUID

import requests

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.constants import MAX_CHUNK_LENGTH
from proxystore.store.base import Store
from proxystore.utils import chunk_bytes
from proxystore.utils import create_key
from proxystore.utils import home_dir

logger = logging.getLogger(__name__)


class EndpointStoreError(Exception):
    """Exception resulting from request to Endpoint."""

    pass


class EndpointStoreKey(NamedTuple):
    """Key to object in an Endpoint."""

    object_id: str
    """Unique object ID."""
    endpoint_id: str | None
    """Endpoint UUID where object is stored."""


class EndpointStore(Store[EndpointStoreKey]):
    """EndpointStore backend class."""

    def __init__(
        self,
        name: str,
        *,
        endpoints: Sequence[str | UUID],
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
            endpoints (sequence): sequence of valid and running endpoint
                UUIDs to use. At least one of these endpoints must be
                accessible by this process.
            proxystore_dir (str): optionally specify the proxystore home
                directory. Defaults to :py:func:`~proxystore.utils.home_dir`.
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
            home_dir() if proxystore_dir is None else proxystore_dir
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

    def create_key(self, obj: Any) -> EndpointStoreKey:
        return EndpointStoreKey(
            object_id=create_key(obj),
            endpoint_id=str(self.endpoint_uuid),
        )

    def evict(self, key: EndpointStoreKey) -> None:
        """Evict object associated with key.

        Args:
            key (EndpointStoreKey): key corresponding to object in store to
                evict.

        Raises:
            EndpointStoreError:
                if the Endpoint returns a non-200 status code.
        """
        response = requests.post(
            f'{self.address}/evict',
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
        )
        if response.status_code != 200:
            raise EndpointStoreError(f'EVICT returned {response}')

        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: EndpointStoreKey) -> bool:
        """Check if key exists.

        Args:
            key (EndpointStoreKey): key to check.

        Raises:
            EndpointStoreError:
                if the Endpoint returns a non-200 status code.
        """
        response = requests.get(
            f'{self.address}/exists',
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
        )
        if response.status_code == 200:
            return response.json()['exists']
        else:
            raise EndpointStoreError(f'EXISTS returned {response}')

    def get_bytes(self, key: EndpointStoreKey) -> bytes | None:
        """Get serialized object from remote store.

        Args:
            key (EndpointStoreError): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist on the endpoint.

        Raises:
            EndpointStoreError:
                if the Endpoint returns a status code other than 200 (success)
                or 400 (missing key).
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
            raise EndpointStoreError(f'GET returned {response}')

    def set_bytes(self, key: EndpointStoreKey, data: bytes) -> None:
        """Set serialized object in remote store with key.

        Args:
            key (EndpointStoreKey): key corresponding to object.
            data (bytes): serialized object.

        Raises:
            EndpointStoreError:
                if the endpoint does not return a 200 status code for success.
        """
        response = requests.post(
            f'{self.address}/set',
            headers={'Content-Type': 'application/octet-stream'},
            params={'key': key.object_id, 'endpoint': key.endpoint_id},
            data=chunk_bytes(data, MAX_CHUNK_LENGTH),
            stream=True,
        )
        if response.status_code != 200:
            raise EndpointStoreError(f'SET returned {response}')

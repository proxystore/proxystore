"""EndpointStore Implementation."""
from __future__ import annotations

import logging
from typing import Any

import requests

import proxystore as ps
from proxystore.endpoint import config
from proxystore.store.base import Store

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
        endpoint_dir: str | None = None,
        hostname: str | None = None,
        port: int | None = None,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init EndpointStore.

        Warning:
            Specifying a custom `endpoint_dir` can cause problems if the
            `endpoint_dir` is not the same on all systems that a proxy created
            by this store could end up on. It is recommended to leave the
            `endpoint_dir` unspecified so ProxyStore can determine the correct
            endpoint to use.

        Args:
            name (str): name of the store instance (default: None).
            endpoint_dir (str): directory containing configuration of
                endpoint to connect to. hostname and port will be taken
                from the config file. If None, defaults to
                :code:`$HOME/.proxystore` (default: None).
            hostname (str): name of the host the endpoint is on. Overrides
                the value found in the endpoint directory (default: None).
            port (int): port the endpoint is listening on. Overrides the
                value found in the endpoint directory (default: None).
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        if (hostname is not None and port is None) or (
            hostname is None and port is not None
        ):
            raise ValueError(
                'Either both or neither of hostname and port must be '
                'specified.',
            )

        if hostname is not None and port is not None:
            self.hostname = hostname
            self.port = port
        else:
            if endpoint_dir is None:
                endpoint_dir_ = config.default_dir()
            else:
                endpoint_dir_ = endpoint_dir
            cfg = config.get_config(endpoint_dir_)
            if cfg.host is None or cfg.port is None:
                raise ValueError(
                    f'Endpoint ({cfg.uuid}) config ({endpoint_dir_}) must '
                    f'have host and port specified. Got host={cfg.host} '
                    f'and port={cfg.port}.',
                )
            self.hostname = cfg.host
            self.port = cfg.port

        self.address = f'http://{self.hostname}:{self.port}'
        self.endpoint_uuid: str

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={
                # Note: don't pass self.hostname, self.port, etc. here because
                # those values may be local to this process or machine and
                # these kwargs may be used by a proxy on a remote machine
                # to reinitialize a store elsewhere using a different endpoint
                'endpoint_dir': endpoint_dir,
                'hostname': hostname,
                'port': port,
            },
        )

        response = requests.get(f'{self.address}/endpoint')
        if response.status_code == 200:
            self.endpoint_uuid = response.json()['uuid']
        else:
            raise EndpointStoreError(
                f'Request for endpoint UUID returned: {response}',
            )

    @staticmethod
    def _create_key(object_key: str, endpoint_uuid: str) -> str:
        return f'{object_key}:{endpoint_uuid}'

    @staticmethod
    def _parse_key(key: str) -> tuple[str, str | None]:
        # TODO: validate format?
        values = key.split(':')
        if len(values) == 1:
            return values[0], None
        elif len(values) == 2:
            return (values[0], values[1])
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
                'endpoint': endpoint_uuid,
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
                'endpoint': endpoint_uuid,
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
                'endpoint': endpoint_uuid,
            },
            stream=True,
        )
        if response.status_code == 200:
            return response.raw.data
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
                'endpoint': endpoint_uuid,
            },
            data=data,
        )
        if response.status_code != 200:
            raise EndpointStoreError(f'SET returned {response}')

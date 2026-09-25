"""Endpoint connector implementation."""

from __future__ import annotations

import collections
import contextlib
import logging
import os
import threading
import uuid
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Self
from uuid import UUID

from proxystore.endpoint.auth import read_token_file
from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import get_configs
from proxystore.endpoint.config import get_token_filepath
from proxystore.endpoint.exceptions import EndpointAuthError
from proxystore.endpoint.exceptions import EndpointClientError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.serialize import BytesLike
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

        # Find the first locally accessible endpoint to use as our
        # home endpoint
        home = (
            home_dir() if self.proxystore_dir is None else self.proxystore_dir
        )
        found: tuple[EndpointConfig, str, EndpointClient] | None = None
        for endpoint in get_configs(home):
            endpoint_uuid = UUID(endpoint.uuid)
            if endpoint_uuid not in self.endpoints:
                continue
            if endpoint.host is None:
                logger.warning(
                    'Found valid configuration for endpoint '
                    f'"{endpoint.name}" ({endpoint_uuid}), but the endpoint '
                    'has not been started',
                )
                continue

            endpoint_dir = os.path.join(home, endpoint.name)
            logger.debug(f'Attempting connection to {endpoint_uuid}')
            try:
                client = _connect(endpoint, endpoint_dir)
            except (EndpointAuthError, EndpointProtocolError) as e:
                logger.warning(
                    f'Connection to {endpoint_uuid} failed: {e}',
                )
                continue
            except (EndpointClientError, OSError, ValueError) as e:
                # OSError includes a missing token file which indicates the
                # endpoint is not running, and ValueError is a malformed
                # token file.
                logger.debug(f'Connection to {endpoint_uuid} failed: {e!r}')
                continue

            if client.info.uuid != endpoint_uuid:
                logger.debug(
                    f'Connection to {endpoint_uuid} returned different UUID',
                )
                client.close()
                continue

            logger.debug(
                f'Connection to {endpoint_uuid} successful, using '
                'as local endpoint',
            )
            found = (endpoint, endpoint_dir, client)
            break

        if found is None:
            raise EndpointConnectorError(
                'Failed to find an endpoint configuration matching one of the '
                'provided endpoint UUIDs, or an endpoint configuration was '
                'found but the endpoint could not be connected to. '
                'Enable debug level logging for more more details.',
            )
        found_config, found_dir, client = found
        self.endpoint_uuid: uuid.UUID = uuid.UUID(found_config.uuid)
        self.endpoint_host: str | None = found_config.host
        self.endpoint_port: int = found_config.port
        self.address = f'{self.endpoint_host}:{self.endpoint_port}'

        self._pool = _ConnectionPool(lambda: _connect(found_config, found_dir))
        self._pool.release(client)

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
        self._pool.close()

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

    @contextlib.contextmanager
    def _request(self, name: str) -> Generator[EndpointClient, None, None]:
        try:
            with self._pool.connection() as client:
                yield client
        except (EndpointClientError, OSError, ValueError) as e:
            raise EndpointConnectorError(f'{name} failed: {e}') from e

    def evict(self, key: EndpointKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        with self._request('Evict') as client:
            client.evict(key.object_id, key.endpoint_id)

    def exists(self, key: EndpointKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        with self._request('Exists') as client:
            return client.exists(key.object_id, key.endpoint_id)

    def get(self, key: EndpointKey) -> BytesLike | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        with self._request('Get') as client:
            return client.get(key.object_id, key.endpoint_id)

    def get_batch(self, keys: Sequence[EndpointKey]) -> list[BytesLike | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def new_key(self, obj: BytesLike | None = None) -> EndpointKey:
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

    def put(self, obj: BytesLike) -> EndpointKey:
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

    def put_batch(self, objs: Sequence[BytesLike]) -> list[EndpointKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]

    def set(self, key: EndpointKey, obj: BytesLike) -> None:
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
        with self._request('Set') as client:
            client.set(key.object_id, obj, key.endpoint_id)


def _connect(config: EndpointConfig, endpoint_dir: str) -> EndpointClient:
    assert config.host is not None
    # The token is read on every connection because it changes each time
    # the endpoint is restarted.
    token = read_token_file(get_token_filepath(endpoint_dir))
    return EndpointClient.connect(config.host, config.port, token)


class _ConnectionPool:
    """Thread-safe pool of connections to an endpoint.

    A connection only processes one request at a time so concurrent requests
    (e.g., from multiple threads) each use a separate connection.

    Args:
        connect: Callable that returns a new connection.
    """

    def __init__(self, connect: Callable[[], EndpointClient]) -> None:
        self._connect = connect
        self._idle: collections.deque[EndpointClient] = collections.deque()
        self._lock = threading.Lock()
        self._pid = os.getpid()

    @contextlib.contextmanager
    def connection(self) -> Generator[EndpointClient, None, None]:
        """Context manager that yields a connection from the pool.

        The connection is returned to the pool on exit unless the connection
        was closed because of an error.
        """
        client = self._acquire()
        try:
            yield client
        finally:
            self.release(client)

    def release(self, client: EndpointClient) -> None:
        """Return a connection to the pool."""
        if client.closed:
            return
        with self._lock:
            if os.getpid() == self._pid:
                self._idle.append(client)
                return
        client.close()

    def close(self) -> None:
        """Close all idle connections in the pool."""
        with self._lock:
            while self._idle:
                self._idle.pop().close()

    def _acquire(self) -> EndpointClient:
        with self._lock:
            if os.getpid() != self._pid:
                # This process was forked so the idle connections are shared
                # with the parent process and cannot be used.
                while self._idle:
                    self._idle.pop().close()
                self._pid = os.getpid()
            if self._idle:
                return self._idle.pop()
        return self._connect()

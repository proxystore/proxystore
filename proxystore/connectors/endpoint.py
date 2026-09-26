"""Endpoint connector implementation."""

from __future__ import annotations

import collections
import logging
import os
import threading
import uuid
import weakref
from collections.abc import Callable
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Self
from typing import TypeVar
from uuid import UUID

from proxystore.endpoint.client import EndpointClient
from proxystore.endpoint.directory import EndpointDir
from proxystore.endpoint.exceptions import EndpointConnectionError
from proxystore.endpoint.exceptions import EndpointConnectorError
from proxystore.endpoint.exceptions import EndpointError
from proxystore.endpoint.exceptions import EndpointProtocolError
from proxystore.serialize import BytesLike
from proxystore.utils.environment import home_dir

logger = logging.getLogger(__name__)

_T = TypeVar('_T')


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
        failures: list[str] = []
        found: tuple[UUID, EndpointDir, EndpointClient] | None = None
        for endpoint_dir, endpoint in EndpointDir.find_all(home):
            endpoint_uuid = UUID(endpoint.uuid)
            if endpoint_uuid not in self.endpoints:
                continue

            logger.debug(f'Attempting connection to {endpoint_uuid}')
            try:
                client = _connect(endpoint_dir, endpoint_uuid)
            except EndpointError as e:
                logger.debug(f'Connection to {endpoint_uuid} failed: {e!r}')
                failures.append(f'{endpoint.name} ({endpoint_uuid}): {e}')
                continue

            logger.debug(
                f'Connection to {endpoint_uuid} successful, using '
                'as local endpoint',
            )
            found = (endpoint_uuid, endpoint_dir, client)
            break

        if found is None:
            if len(failures) == 0:
                raise EndpointConnectorError(
                    'Failed to find an endpoint configuration in '
                    f'{home} matching one of the provided endpoint UUIDs.',
                )
            reasons = '\n'.join(f'  - {failure}' for failure in failures)
            raise EndpointConnectorError(
                'Failed to connect to any of the endpoints matching the '
                f'provided endpoint UUIDs:\n{reasons}',
            )
        endpoint_uuid, endpoint_dir, client = found
        self.endpoint_uuid: uuid.UUID = endpoint_uuid
        self.endpoint_dir = endpoint_dir

        self._pool = _ConnectionPool(
            lambda: _connect(endpoint_dir, endpoint_uuid),
        )
        self._pool.add(client)

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
            f'in {self.endpoint_dir})'
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

    def _request(
        self,
        name: str,
        request: Callable[[EndpointClient], _T],
    ) -> _T:
        try:
            return self._pool.run(request)
        except (EndpointError, ValueError) as e:
            raise EndpointConnectorError(f'{name} failed: {e}') from e

    def evict(self, key: EndpointKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        self._request(
            'Evict',
            lambda client: client.evict(key.object_id, key.endpoint_id),
        )

    def exists(self, key: EndpointKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        return self._request(
            'Exists',
            lambda client: client.exists(key.object_id, key.endpoint_id),
        )

    def get(self, key: EndpointKey) -> BytesLike | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        return self._request(
            'Get',
            lambda client: client.get(key.object_id, key.endpoint_id),
        )

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
        self._request(
            'Set',
            lambda client: client.set(key.object_id, obj, key.endpoint_id),
        )


def _connect(endpoint_dir: EndpointDir, endpoint_uuid: UUID) -> EndpointClient:
    client = EndpointClient.from_dir(endpoint_dir)
    if client.info.uuid != endpoint_uuid:
        client.close()
        raise EndpointProtocolError(
            f'Expected endpoint {endpoint_uuid} but the endpoint running in '
            f'{endpoint_dir} is {client.info.uuid}.',
        )
    return client


class _ConnectionPool:
    """Thread-safe pool of connections to an endpoint.

    A connection only processes one request at a time so concurrent requests
    (e.g., from multiple threads) each use a separate connection. The pool
    does not limit the number of connections, so it holds at most one idle
    connection for each request that was made concurrently.

    Args:
        connect: Callable that returns a new connection.
    """

    def __init__(self, connect: Callable[[], EndpointClient]) -> None:
        self._connect = connect
        self._idle: collections.deque[EndpointClient] = collections.deque()
        self._lock = threading.Lock()
        _POOLS.add(self)

    def run(self, request: Callable[[EndpointClient], _T]) -> _T:
        """Run a request with a connection from the pool.

        Idle connections can be closed by the endpoint (e.g., when the
        endpoint is restarted), so a request that fails because an idle
        connection was closed is retried once with a new connection. All
        requests are safe to retry because objects are write-once.

        Args:
            request: Callable that makes a request with a connection.

        Returns:
            The result of the request.
        """
        client, reused = self._acquire()
        try:
            return request(client)
        except EndpointConnectionError:
            if not reused:
                raise
            logger.debug(
                'Retrying request with a new connection because an idle '
                'connection to the endpoint was closed',
            )
        finally:
            self._release(client)

        client = self._connect()
        try:
            return request(client)
        finally:
            self._release(client)

    def close(self) -> None:
        """Close all idle connections in the pool."""
        with self._lock:
            while self._idle:
                self._idle.pop().close()

    def add(self, client: EndpointClient) -> None:
        """Add an idle connection to the pool."""
        self._release(client)

    def _acquire(self) -> tuple[EndpointClient, bool]:
        with self._lock:
            if self._idle:
                return self._idle.pop(), True
        return self._connect(), False

    def _release(self, client: EndpointClient) -> None:
        if client.closed:
            return
        with self._lock:
            self._idle.append(client)

    def _reset_after_fork(self) -> None:
        # The idle connections are shared with the parent process so they
        # cannot be used by the child, and the lock may have been held by
        # another thread of the parent when the process was forked.
        self._lock = threading.Lock()
        while self._idle:
            self._idle.pop().close()


_POOLS: weakref.WeakSet[_ConnectionPool] = weakref.WeakSet()


def _reset_pools_after_fork() -> None:
    for pool in list(_POOLS):
        pool._reset_after_fork()


if hasattr(os, 'register_at_fork'):  # pragma: no branch
    os.register_at_fork(after_in_child=_reset_pools_after_fork)

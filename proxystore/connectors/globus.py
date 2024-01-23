"""Globus transfer connector implementation."""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Collection
from typing import Generator
from typing import Iterator
from typing import Literal
from typing import NamedTuple
from typing import Pattern
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import globus_sdk

from proxystore.globus.transfer import get_transfer_client_flow
from proxystore.utils.environment import hostname

logger = logging.getLogger(__name__)
GLOBUS_MKDIR_EXISTS_ERROR_CODE = 'ExternalError.MkdirFailed.Exists'

SerializerT = Callable[[Any], bytes]


class GlobusEndpoint:
    """Globus endpoint representation.

    Args:
        uuid: UUID of Globus endpoint.
        endpoint_path: Path within endpoint to directory to use
            for storing objects.
        local_path: Local path (as seen by the host filesystem) that
            corresponds to the directory specified by `endpoint_path`.
        host_regex: String that matches the host where
            the Globus endpoint exists or regex pattern than can be used
            to match the host. The host pattern is needed so that proxies
            can figure out what the local endpoint is when they are resolved.
    """

    def __init__(
        self,
        uuid: str,
        endpoint_path: str,
        local_path: str | None,
        host_regex: str | Pattern[str],
    ) -> None:
        if not isinstance(uuid, str):
            raise TypeError('uuid must be a str.')
        if not isinstance(endpoint_path, str):
            raise TypeError('endpoint_path must be a str.')
        if not isinstance(local_path, str):
            raise TypeError('local_path must be a str.')
        if not isinstance(host_regex, (str, Pattern)):
            raise TypeError('host_regex must be a str or re.Pattern.')

        self.uuid = uuid
        self.endpoint_path = endpoint_path
        self.local_path = local_path
        self.host_regex = host_regex

    def __eq__(self, endpoint: object) -> bool:
        if not isinstance(endpoint, GlobusEndpoint):
            raise NotImplementedError
        if (
            self.uuid == endpoint.uuid
            and self.endpoint_path == endpoint.endpoint_path
            and self.local_path == endpoint.local_path
            and self.host_regex == endpoint.host_regex
        ):
            return True
        return False

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(uuid='{self.uuid}', "
            f"endpoint_path='{self.endpoint_path}', "
            f"local_path='{self.local_path}', "
            f"host_regex='{self.host_regex}')"
        )


class GlobusEndpoints:
    """A collection of Globus endpoints.

    Args:
        endpoints: Iterable of
            [`GlobusEndpoints`][proxystore.connectors.globus.GlobusEndpoints]
            instances.

    Raises:
        ValueError: If `endpoints` has length 0 or if multiple endpoints with \
            the same UUID are provided.
    """

    def __init__(self, endpoints: Collection[GlobusEndpoint]) -> None:
        if len(endpoints) == 0:
            raise ValueError(
                'GlobusEndpoints must be passed at least one GlobusEndpoint '
                'object',
            )
        self._endpoints: dict[str, GlobusEndpoint] = {}
        for endpoint in endpoints:
            if endpoint.uuid in self._endpoints:
                raise ValueError(
                    'Cannot pass multiple GlobusEndpoint objects with the '
                    'same Globus endpoint UUID.',
                )
            self._endpoints[endpoint.uuid] = endpoint

    def __getitem__(self, uuid: str) -> GlobusEndpoint:
        try:
            return self._endpoints[uuid]
        except KeyError:
            raise KeyError(
                f'Endpoint with UUID {uuid} does not exist.',
            ) from None

    def __iter__(self) -> Iterator[GlobusEndpoint]:
        def _iterator() -> Generator[GlobusEndpoint, None, None]:
            yield from self._endpoints.values()

        return _iterator()

    def __len__(self) -> int:
        return len(self._endpoints)

    def __repr__(self) -> str:
        s = f'{self.__class__.__name__}(['
        s += ', '.join(str(ep) for ep in self._endpoints.values())
        s += '])'
        return s

    @classmethod
    def from_dict(
        cls: type[GlobusEndpoints],
        json_object: dict[str, dict[str, str]],
    ) -> GlobusEndpoints:
        """Construct an endpoints collection from a dictionary.

        Example:

            ```python
            {
              "endpoint-uuid-1": {
                "host_regex": "host1-regex",
                "endpoint_path": "/path/to/endpoint/dir",
                "local_path": "/path/to/local/dir"
              },
              "endpoint-uuid-2": {
                "host_regex": "host2-regex",
                "endpoint_path": "/path/to/endpoint/dir",
                "local_path": "/path/to/local/dir"
              }
            }
            ```
        """  # noqa: D412
        endpoints = []
        for ep_uuid, params in json_object.items():
            endpoints.append(
                GlobusEndpoint(
                    uuid=ep_uuid,
                    endpoint_path=params['endpoint_path'],
                    local_path=params['local_path'],
                    host_regex=params['host_regex'],
                ),
            )
        return GlobusEndpoints(endpoints)

    @classmethod
    def from_json(cls, json_file: str) -> GlobusEndpoints:
        """Construct a GlobusEndpoints object from a json file.

        The `dict` read from the JSON file will be passed to
        [`from_dict()`][proxystore.connectors.globus.GlobusEndpoints.from_dict]
        and should match the format expected by
        [`from_dict()`][proxystore.connectors.globus.GlobusEndpoints.from_dict].
        """
        with open(json_file) as f:
            data = f.read()
        return cls.from_dict(json.loads(data))

    def dict(self) -> dict[str, dict[str, str]]:
        """Convert the GlobusEndpoints to a dict.

        Note that the
        [`GlobusEndpoints`][proxystore.connectors.globus.GlobusEndpoints]
        object can be reconstructed by passing the `dict` to.
        [`from_dict()`][proxystore.connectors.globus.GlobusEndpoints.from_dict].
        """
        data = {}
        for endpoint in self:
            data[endpoint.uuid] = {
                'endpoint_path': endpoint.endpoint_path,
                'local_path': endpoint.local_path,
                'host_regex': endpoint.host_regex.pattern
                if isinstance(endpoint.host_regex, Pattern)
                else endpoint.host_regex,
            }
        return data

    def get_by_host(self, host: str) -> GlobusEndpoint:
        """Get endpoint by host.

        Searches the endpoints for a endpoint who's `host_regex` matches
        `host`.

        Args:
            host: Host to match.

        Returns:
            Globus endpoint.

        Raises:
            ValueError: If `host` does not match any of the endpoints.
        """
        for endpoint in self._endpoints.values():
            if re.fullmatch(endpoint.host_regex, host) is not None:
                return endpoint
        raise ValueError(f'Cannot find endpoint matching host {host}')


class GlobusKey(NamedTuple):
    """Key to object transferred with Globus.

    Attributes:
        filename: Unique object filename.
        task_id: Globus transfer task IDs for the file.
    """

    filename: str
    # We support single strings for backwards compatibility with
    # proxies created in v0.5.1 or older.
    task_id: str | tuple[str, ...]

    def __eq__(self, other: Any) -> bool:
        """Match keys by filename only.

        This is a hack around the fact that the task_id is not created until
        after the filename is so there can be a state where the task_id
        is empty.
        """
        if isinstance(other, tuple):
            return self[0] == other[0]
        return False

    def __ne__(self, other: Any) -> bool:
        # Match keys by filename only.
        return not self == other


class GlobusConnector:
    """Globus transfer connector.

    The [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector] is
    similar to a [`FileConnector`][proxystore.connectors.file.FileConnector]
    in that objects are saved to disk but allows for the transfer of objects
    between two remote file systems. The two directories on the separate file
    systems are kept in sync via Globus transfers. The
    [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector]
    is useful when moving data between hosts that have a Globus endpoint but
    may have restrictions that prevent the use of other store backends
    (e.g., ports cannot be opened for using a
    [`RedisConnector`][proxystore.connectors.redis.RedisConnector].

    Note:
        To use Globus for data transfer, Globus authentication needs to be
        performed with the `#!bash proxystore-globus-auth` CLI. If
        authentication is not performed before initializing a
        [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector],
        the program will prompt the user to perform authentication. This can
        result in unexpected program hangs while the constructor waits on the
        user to authenticate. Authentication only needs to be performed once
        per system

    Args:
        endpoints: Globus endpoints to keep in sync. If passed as a `dict`,
            the dictionary must match the format expected by
            [`GlobusEndpoints.from_dict()`][proxystore.connectors.globus.GlobusEndpoints.from_dict].
            Note that given `n` endpoints there will be `n-1` Globus transfers
            per operation, so we suggest not using too many endpoints at the
            same time.
        polling_interval: Interval in seconds to check if Globus
            tasks have finished.
        sync_level: Globus transfer sync level.
        timeout: Timeout in seconds for waiting on Globus tasks.
        clear: Clear all objects on
            [`close()`][proxystore.connectors.globus.GlobusConnector.close] by
            deleting the `local_path` of each endpoint.

    Raises:
        GlobusAuthFileError: If the Globus authentication file cannot be found.
        ValueError: If `endpoints` is of an incorrect type.
        ValueError: If fewer than two endpoints are provided.
    """

    def __init__(
        self,
        endpoints: GlobusEndpoints
        | list[GlobusEndpoint]
        | dict[str, dict[str, str]],
        polling_interval: int = 1,
        sync_level: int
        | Literal['exists', 'size', 'mtime', 'checksum'] = 'mtime',
        timeout: int = 60,
        clear: bool = True,
    ) -> None:
        if isinstance(endpoints, GlobusEndpoints):
            self.endpoints = endpoints
        elif isinstance(endpoints, list):
            self.endpoints = GlobusEndpoints(endpoints)
        elif isinstance(endpoints, dict):
            self.endpoints = GlobusEndpoints.from_dict(endpoints)
        else:
            raise ValueError(
                'endpoints must be of type GlobusEndpoints or a list of '
                f'GlobusEndpoint. Got {type(endpoints)}.',
            )
        if len(endpoints) < 2:
            raise ValueError('At least two Globus endpoints are required.')
        self.polling_interval = polling_interval
        self.sync_level = sync_level
        self.timeout = timeout
        self.clear = clear

        self._transfer_client = get_transfer_client_flow(
            check_collections=[ep.uuid for ep in self.endpoints],
        )

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
        return f'{self.__class__.__name__}(endpoints={self.endpoints})'

    def _get_filepath(
        self,
        filename: str,
        endpoint: GlobusEndpoint | None = None,
    ) -> str:
        """Get filepath from filename.

        Args:
            filename: Name of file in Globus.
            endpoint: Optionally specify a GlobusEndpoint
                to get the filepath relative to. If not specified, the endpoint
                associated with the local host will be used.

        Returns:
            Full local path to file.
        """
        if endpoint is None:
            endpoint = self._get_local_endpoint()
        local_path = os.path.expanduser(endpoint.local_path)
        return os.path.join(local_path, filename)

    def _get_local_endpoint(self) -> GlobusEndpoint:
        """Get endpoint local to current host."""
        return self.endpoints.get_by_host(hostname())

    def _validate_task_id(self, task_ids: str | tuple[str, ...]) -> bool:
        """Validate key contains a real Globus task id."""
        task_ids = task_ids if isinstance(task_ids, tuple) else (task_ids,)
        for tid in task_ids:
            try:
                self._transfer_client.get_task(tid)
            except globus_sdk.TransferAPIError as e:
                if e.http_status == 400:
                    return False
                raise e
        return True

    def _wait_on_tasks(self, task_ids: str | tuple[str, ...]) -> None:
        """Wait on list of Globus tasks."""
        task_ids = task_ids if isinstance(task_ids, tuple) else (task_ids,)
        for tid in task_ids:
            done = self._transfer_client.task_wait(
                tid,
                timeout=self.timeout,
                polling_interval=self.polling_interval,
            )
            if not done:
                raise RuntimeError(
                    f'Task {tid} did not complete within the timeout',
                )

    def _transfer_files(
        self,
        filenames: str | list[str],
        delete: bool = False,
    ) -> tuple[str, ...]:
        """Launch Globus Transfers to sync endpoints.

        Args:
            filenames: Filename or list of filenames to transfer.
                Note must be filenames, not filepaths.
            delete: If `True`, delete the filenames rather than syncing them.

        Returns:
            Tuple of Globus Task UUID that can be used to check the status of
            the transfers.
        """
        src_endpoint = self._get_local_endpoint()
        dst_endpoints = [ep for ep in self.endpoints if ep != src_endpoint]
        tids: list[str] = []

        for dst_endpoint in dst_endpoints:
            transfer_task: globus_sdk.DeleteData | globus_sdk.TransferData
            if delete:
                transfer_task = globus_sdk.DeleteData(
                    self._transfer_client,
                    endpoint=dst_endpoint.uuid,
                )
            else:
                transfer_task = globus_sdk.TransferData(
                    self._transfer_client,
                    source_endpoint=src_endpoint.uuid,
                    destination_endpoint=dst_endpoint.uuid,
                    sync_level=self.sync_level,
                )

            transfer_task['notify_on_succeeded'] = False
            transfer_task['notify_on_failed'] = False
            transfer_task['notify_on_inactive'] = False

            if isinstance(filenames, str):
                filenames = [filenames]

            for filename in filenames:
                src_path = os.path.join(src_endpoint.endpoint_path, filename)
                dst_path = os.path.join(dst_endpoint.endpoint_path, filename)

                if isinstance(transfer_task, globus_sdk.DeleteData):
                    transfer_task.add_item(path=dst_path)
                elif isinstance(transfer_task, globus_sdk.TransferData):
                    transfer_task.add_item(
                        source_path=src_path,
                        destination_path=dst_path,
                    )
                else:
                    raise AssertionError('Unreachable.')

            tdata = _submit_transfer_action(
                self._transfer_client,
                transfer_task,
            )
            tids.append(tdata['task_id'])

        return tuple(tids)

    def close(self, clear: bool | None = None) -> None:
        """Close the connector and clean up.

        Warning:
            This will delete the directory at `local_path` on each endpoint
            by default.

        Warning:
            This method should only be called at the end of the program when
            the store will no longer be used, for example once all proxies
            have been resolved.

        Args:
            clear: Remove the store directory. Overrides the default
                value of `clear` provided when the
                [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector]
                was instantiated.
        """
        clear = self.clear if clear is None else clear
        if clear:
            for endpoint in self.endpoints:
                delete_task = globus_sdk.DeleteData(
                    self._transfer_client,
                    endpoint=endpoint.uuid,
                    recursive=True,
                )
                delete_task['notify_on_succeeded'] = False
                delete_task['notify_on_failed'] = False
                delete_task['notify_on_inactive'] = False
                delete_task.add_item(endpoint.endpoint_path)
                tdata = _submit_transfer_action(
                    self._transfer_client,
                    delete_task,
                )
                self._wait_on_tasks(tdata['task_id'])

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'endpoints': self.endpoints.dict(),
            'polling_interval': self.polling_interval,
            'sync_level': self.sync_level,
            'timeout': self.timeout,
            'clear': self.clear,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> GlobusConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: GlobusKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        if not self.exists(key):
            return

        path = self._get_filepath(key.filename)
        os.remove(path)
        self._transfer_files(key.filename, delete=True)

    def exists(self, key: GlobusKey) -> bool:
        """Check if an object associated with the key exists.

        Note:
            If the corresponding Globus transfer is still in progress, this
            method will wait to make sure the transfers is successful.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        if not self._validate_task_id(key.task_id):
            return False
        self._wait_on_tasks(key.task_id)
        return os.path.exists(self._get_filepath(key.filename))

    def get(self, key: GlobusKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        if not self.exists(key):
            return None

        path = self._get_filepath(key.filename)
        with open(path, 'rb') as f:
            return f.read()

    def get_batch(self, keys: Sequence[GlobusKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def put(self, obj: bytes) -> GlobusKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        filename = str(uuid.uuid4())

        path = self._get_filepath(filename)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'wb', buffering=0) as f:
            f.write(obj)

        tids = self._transfer_files(filename)

        return GlobusKey(filename=filename, task_id=tids)

    def put_batch(self, objs: Sequence[bytes]) -> list[GlobusKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        filenames = [str(uuid.uuid4()) for _ in objs]

        for filename, obj in zip(filenames, objs):
            path = self._get_filepath(filename)
            os.makedirs(os.path.dirname(path), exist_ok=True)

            with open(path, 'wb', buffering=0) as f:
                f.write(obj)

        tids = self._transfer_files(filenames)

        return [
            GlobusKey(filename=filename, task_id=tids)
            for filename in filenames
        ]


def _submit_transfer_action(
    client: globus_sdk.TransferClient,
    task: globus_sdk.DeleteData | globus_sdk.TransferData,
) -> globus_sdk.response.GlobusHTTPResponse:
    """Submit Globus transfer task via the client.

    This helper function primarily adds some additional feedback on raised
    exceptions.

    Args:
        client: Globus transfer client.
        task: Globus transfer task.

    Returns:
        A `GlobusHTTPResponse`.
    """
    try:
        if isinstance(task, globus_sdk.DeleteData):
            response = client.submit_delete(task)
            logger.debug(
                'Submitted DeleteData Globus task with ID '
                f'{response["task_id"]}',
            )
            return response
        elif isinstance(task, globus_sdk.TransferData):
            response = client.submit_transfer(task)
            logger.debug(
                'Submitted TransferData Globus task with ID '
                f'{response["task_id"]}',
            )
            return response
        else:
            raise AssertionError('Unreachable.')
    except globus_sdk.TransferAPIError as e:  # pragma: no cover
        # https://github.com/globus/globus-sdk-python/blob/054a29167c86f66b77bb99beca45ce317b02a5a7/src/globus_sdk/exc/err_info.py#L93  # noqa: E501
        raise Exception(
            f'Failure initiating Globus Transfer. Error info: {e.info}',
        ) from e

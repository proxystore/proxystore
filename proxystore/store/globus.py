"""Globus Endpoint Implementation."""
from __future__ import annotations

import json
import logging
import os
import re
import socket
import sys
import time
import warnings
from typing import Any
from typing import Collection
from typing import Generator
from typing import Iterator
from typing import Pattern
from typing import Sequence

if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
    from typing import Literal
else:  # pragma: <3.9 cover
    from typing_extensions import Literal

import globus_sdk
from parsl.data_provider import globus

import proxystore as ps
from proxystore.store.base import Store

logger = logging.getLogger(__name__)
GLOBUS_MKDIR_EXISTS_ERROR_CODE = 'ExternalError.MkdirFailed.Exists'


class GlobusEndpoint:
    """GlobusEndpoint Class."""

    def __init__(
        self,
        uuid: str,
        endpoint_path: str,
        local_path: str | None,
        host_regex: str | Pattern[str],
    ) -> None:
        """Init GlobusEndpoint.

        Args:
            uuid (str): UUID of Globus endpoint.
            endpoint_path (str): path within endpoint to directory to use
                for storing objects.
            local_path (str): local path (as seen by the host filesystem) that
                corresponds to the directory specified by `endpoint_path`.
            host_regex (str, Pattern): `str` that matches the host where
                the Globus endpoint exists or regex pattern than can be used
                to match the host. The host pattern is needed so that proxies
                can figure out what the local endpoint is when they are
                resolved.
        """
        if not isinstance(uuid, str):
            raise TypeError('uuid must be a str.')
        if not isinstance(endpoint_path, str):
            raise TypeError('endpoint_path must be a str.')
        if not isinstance(local_path, str):
            raise TypeError('local_path must be a str.')
        if not (
            isinstance(host_regex, str) or isinstance(host_regex, Pattern)
        ):
            raise TypeError('host_regex must be a str or re.Pattern.')

        self.uuid = uuid
        self.endpoint_path = endpoint_path
        self.local_path = local_path
        self.host_regex = host_regex

    def __eq__(self, endpoint: object) -> bool:
        """Endpoints are equal if attributes match."""
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
        """Represent GlobusEndpoint as string."""
        return (
            f"{self.__class__.__name__}(uuid='{self.uuid}', "
            f"endpoint_path='{self.endpoint_path}', "
            f"local_path='{self.local_path}', "
            f"host_regex='{self.host_regex}')"
        )


class GlobusEndpoints:
    """GlobusEndpoints Class."""

    def __init__(self, endpoints: Collection[GlobusEndpoint]) -> None:
        """Init GlobusEndpoints.

        Args:
            endpoints: iterable of
                :class:`GlobusEndpoint <.GlobusEndpoint>` instances.

        Raises:
            ValueError:
                if `endpoints` has length 0 or if multiple endpoints with the
                same UUID are provided.
        """
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
        """Index GlobusEndpoints with UUID."""
        try:
            return self._endpoints[uuid]
        except KeyError:
            raise KeyError(f'Endpoint with UUID {uuid} does not exist.')

    def __iter__(self) -> Iterator[GlobusEndpoint]:
        """Iterate over GlobusEndpoints."""

        def _iterator() -> Generator[GlobusEndpoint, None, None]:
            yield from self._endpoints.values()

        return _iterator()

    def __len__(self) -> int:
        """Length of GlobusEndpoints."""
        return len(self._endpoints)

    def __repr__(self) -> str:
        """Represent GlobusEndpoints as string."""
        s = f'{self.__class__.__name__}(['
        s += ', '.join(str(ep) for ep in self._endpoints.values())
        s += '])'
        return s

    @classmethod
    def from_dict(
        cls: type[GlobusEndpoints],
        json_object: dict[str, dict[str, str]],
    ) -> GlobusEndpoints:
        """Construct a GlobusEndpoints object from a dictionary.

        Example:

        .. code-block:: text

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
        """  # noqa: D412
        endpoints = []
        for uuid, params in json_object.items():
            endpoints.append(
                GlobusEndpoint(
                    uuid=uuid,
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
        :func:`from_dict()` and should match the format expected by
        :func:`from_dict()`.
        """
        with open(json_file) as f:
            data = f.read()
        return cls.from_dict(json.loads(data))

    def dict(self) -> dict[str, dict[str, str]]:
        """Convert the GlobusEndpoints to a dict.

        Note that the :class:`.GlobusEndpoints` object can be reconstructed by
        passing the `dict` to :func:`from_dict()`.
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
            host (str): host to match/

        Returns:
            :class:`GlobusEndpoint <.GlobusEndpoint>`

        Raises:
            ValueError:
                if `host` does not match any of the endpoints.
        """
        for endpoint in self._endpoints.values():
            if re.fullmatch(endpoint.host_regex, host) is not None:
                return endpoint
        raise ValueError(f'Cannot find endpoint matching host {host}')


class GlobusStore(Store):
    """Globus backend class.

    The :class:`GlobusStore <.GlobusStore>` is similar to a
    :class:`FileStore <proxystore.store.file.FileStore>` in that objects in the
    store are saved to disk but allows for the transfer of objects between two
    remote file systems. The two directories on the separate file systems are
    kept in sync via Globus transfers. The :class:`GlobusStore <.GlobusStore>`
    is useful when moving data between hosts that have a Globus endpoint but
    may have restrictions that prevent the use of other store backends
    (e.g., ports cannot be opened for using a
    :class:`RedisStore <proxystore.store.redis.RedisStore>`).

    Note:
        To use Globus for data transfer, Globus authentication needs to be
        performed. The user will be prompted to authenticate when the
        :class:`GlobusStore <.GlobusStore>` is initialized. Alternatively,
        authentication can be performed on the command line with
        :code:`$ parsl_globus_auth`. Note authentication only needs to be
        performed once.

    Warning:
        The :class:`GlobusStore <.GlobusStore>` encodes the Globus transfer
        IDs into the keys, thus the keys returned by functions such
        as :func:`set() <set>` will be different.

    Warning:
        :class:`GlobusStore <.GlobusStore>` enforces strict guarantees on
        object versions. I.e., the parameter :code:`strict` will be ignored
        and objects returned by the store will always be the most up to date
        version.
    """

    def __init__(
        self,
        name: str,
        *,
        endpoints: GlobusEndpoints
        | list[GlobusEndpoint]
        | dict[str, dict[str, str]],
        polling_interval: int = 1,
        sync_level: int
        | Literal['exists', 'size', 'mtime', 'checksum'] = 'mtime',
        timeout: int = 60,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init GlobusStore.

        Args:
            name (str): name of the store instance.
            endpoints (GlobusEndpoints): Globus endpoints to keep
                in sync. If passed as a `dict`, the dictionary must match the
                format expected by :func:`GlobusEndpoints.from_dict()`.
            polling_interval (int): interval in seconds to check if Globus
                tasks have finished.
            sync_level (str, int): Globus transfer sync level.
            timeout (int): timeout in seconds for waiting on Globus tasks.
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).

        Raise:
            ValueError:
                if `endpoints` is not a list of
                :class:`GlobusEndpoint <.GlobusEndpoint>`, instance of
                :class:`GlobusEndpoints <.GlobusEndpoints>`, or dict.
            ValueError:
                if the :code:`len(endpoints) != 2` because
                :class:`GlobusStore <.GlobusStore>` can currently only keep
                two endpoints in sync.
        """
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
        if len(endpoints) != 2:
            raise ValueError(
                'ProxyStore only supports two endpoints at a time',
            )
        self.polling_interval = polling_interval
        self.sync_level = sync_level
        self.timeout = timeout

        parsl_globus_auth = globus.get_globus()

        self._transfer_client = globus_sdk.TransferClient(
            authorizer=parsl_globus_auth.authorizer,
        )

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={
                # Pass endpoints as a dict to make kwargs JSON serializable
                'endpoints': self.endpoints.dict(),
                'polling_interval': self.polling_interval,
                'sync_level': self.sync_level,
                'timeout': self.timeout,
            },
        )

    def _create_key(self, filename: str, task_id: str) -> str:
        """Create key for GlobusStore.

        Args:
            filename (str): name of file in Globus.
            task_id (str): Globus task id that should be waited on before
                accessing `filename`.

        Returns:
            key that encodes the `filename` and `task_id`.
        """
        return f'{task_id}:{filename}'

    def _get_filename(self, key: str) -> str:
        """Extract filename from key."""
        return key.split(':')[1]

    def _get_filepath(
        self,
        filename: str,
        endpoint: GlobusEndpoint | None = None,
    ) -> str:
        """Get filepath from filename.

        Args:
            filename (str): name of file in Globus
            endpoint (GlobusEndpoint): optionally specify a GlobusEndpoint
                to get the filepath relative to. If not specified, the endpoint
                associated with the local host will be used.

        Returns:
            full local path to file.
        """
        if endpoint is None:
            endpoint = self._get_local_endpoint()
        local_path = os.path.expanduser(endpoint.local_path)
        return os.path.join(local_path, filename)

    def _get_local_endpoint(self) -> GlobusEndpoint:
        """Get endpoint local to current host."""
        return self.endpoints.get_by_host(socket.gethostname())

    def _get_task_id(self, key: str) -> str:
        """Extract task id from key."""
        return key.split(':')[0]

    def _validate_key(self, key: str) -> bool:
        """Validate key contains a real Globus task id."""
        if len(key.split(':')) != 2:
            return False
        try:
            self._transfer_client.get_task(self._get_task_id(key))
        except globus_sdk.TransferAPIError as e:
            if e.http_status == 400:
                return False
            raise e
        return True

    def _wait_on_tasks(self, *tasks: str) -> None:
        """Wait on list of Globus tasks."""
        for task in tasks:
            done = self._transfer_client.task_wait(
                task,
                timeout=self.timeout,
                polling_interval=self.polling_interval,
            )
            if not done:
                raise RuntimeError(
                    f'Task {task} did not complete within the ' 'timeout',
                )

    def _transfer_files(
        self,
        filenames: str | list[str],
        delete: bool = False,
    ) -> str:
        """Launch Globus Transfer to sync endpoints.

        Args:
            filenames (str, list): filename or list of filenames to transfer.
                Note must be filenames, not filepaths.
            delete (bool): if `True`, delete the filenames rather than syncing
                them.

        Returns:
            Globus Task UUID that can be used to check the status of the
            transfer.
        """
        src_endpoint = self._get_local_endpoint()
        dst_endpoints = [ep for ep in self.endpoints if ep != src_endpoint]
        assert len(dst_endpoints) == 1
        dst_endpoint = dst_endpoints[0]

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
            if isinstance(transfer_task, globus_sdk.DeleteData):
                transfer_task.add_item(
                    path=os.path.join(dst_endpoint.endpoint_path, filename),
                )
            elif isinstance(transfer_task, globus_sdk.TransferData):
                transfer_task.add_item(
                    source_path=os.path.join(
                        src_endpoint.endpoint_path,
                        filename,
                    ),
                    destination_path=os.path.join(
                        dst_endpoint.endpoint_path,
                        filename,
                    ),
                )
            else:
                raise AssertionError('Unreachable.')

        if isinstance(transfer_task, globus_sdk.DeleteData):
            tdata = self._transfer_client.submit_delete(transfer_task)
        elif isinstance(transfer_task, globus_sdk.TransferData):
            tdata = self._transfer_client.submit_transfer(transfer_task)
        else:
            raise AssertionError('Unreachable.')

        return tdata['task_id']

    def close(self) -> None:
        """Cleanup directories used by ProxyStore in the Globus endpoints.

        Warning:
            Will delete the directory at `local_path` on each endpoint.

        Warning:
            This method should only be called at the end of the program when
            the store will no longer be used, for example once all proxies
            have been resolved.
        """
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
            tdata = self._transfer_client.submit_delete(delete_task)
            self._wait_on_tasks(tdata['task_id'])

    def evict(self, key: str) -> None:
        if not self.exists(key):
            return

        filename = self._get_filename(key)
        path = self._get_filepath(filename)
        os.remove(path)
        self._cache.evict(key)
        self._transfer_files(filename, delete=True)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        if not self._validate_key(key):
            return False
        self._wait_on_tasks(self._get_task_id(key))
        return os.path.exists(self._get_filepath(self._get_filename(key)))

    def get_bytes(self, key: str) -> bytes | None:
        if not self.exists(key):
            return None

        path = self._get_filepath(self._get_filename(key))
        with open(path, 'rb') as f:
            return f.read()

    def get_timestamp(self, key: str) -> float:
        if not self.exists(key):
            raise KeyError(
                f"Key='{key}' does not have a corresponding file in the store",
            )
        return os.path.getmtime(self._get_filepath(self._get_filename(key)))

    def get(
        self,
        key: str,
        *,
        deserialize: bool = True,
        strict: bool = False,
        default: Any | None = None,
    ) -> Any | None:
        if strict:
            warnings.warn(
                'GlobusStore objects are immutable so setting strict=True '
                'has no effect.',
            )
        return super().get(
            key,
            deserialize=deserialize,
            strict=False,
            default=default,
        )

    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        return self._cache.exists(key)

    def set_bytes(self, key: str, data: bytes) -> None:
        if not isinstance(data, bytes):
            raise TypeError(f'data must be of type bytes. Found {type(data)}')
        path = self._get_filepath(key)
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb', buffering=0) as f:
            f.write(data)
        # Manually set timestamp on file with nanosecond precision because some
        # filesystems can have low default file modified precisions
        timestamp = time.time_ns()
        os.utime(path, ns=(timestamp, timestamp))

    def set(
        self,
        obj: Any,
        *,
        key: str | None = None,
        serialize: bool = True,
    ) -> str:
        if serialize:
            obj = ps.serialize.serialize(obj)
        if key is None:
            filename = self.create_key(obj)
        else:
            filename = key

        self.set_bytes(filename, obj)
        tid = self._transfer_files(filename)
        key = self._create_key(filename=filename, task_id=tid)
        logger.debug(
            f"SET key='{key}' IN {self.__class__.__name__}"
            f"(name='{self.name}')",
        )
        return key

    def set_batch(
        self,
        objs: Sequence[Any],
        *,
        keys: Sequence[str | None] | None = None,
        serialize: bool = True,
    ) -> list[str]:
        if keys is not None and len(objs) != len(keys):
            raise ValueError(
                f'objs has length {len(objs)} but keys has length {len(keys)}',
            )
        if keys is None:
            keys = [None] * len(objs)

        filenames = []
        for obj, key in zip(objs, keys):
            if serialize:
                obj = ps.serialize.serialize(obj)
            if key is None:
                filename = self.create_key(obj)
            else:
                filename = key
            filenames.append(filename)

            self.set_bytes(filename, obj)

        # Batch of objs written to disk so we can trigger Globus transfer
        tid = self._transfer_files(filenames)

        final_keys = []
        for filename in filenames:
            key = self._create_key(filename=filename, task_id=tid)
            logger.debug(
                f"SET key='{key}' IN {self.__class__.__name__}"
                f"(name='{self.name}')",
            )
            final_keys.append(key)

        return final_keys

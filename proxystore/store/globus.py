"""Globus Endpoint Implementation"""
from __future__ import annotations

import logging
import json
import os
import re
import socket
import time
import warnings

from typing import Any, Dict, Iterable, List, Optional, Pattern, Union

import proxystore as ps
from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.store.remote import RemoteFactory, RemoteStore

import_error = None
try:
    import globus_sdk
    from parsl.data_provider import globus
except ImportError as e:  # pragma: no cover
    import_error = e

logger = logging.getLogger(__name__)
GLOBUS_MKDIR_EXISTS_ERROR_CODE = "ExternalError.MkdirFailed.Exists"


class GlobusEndpoint:
    """GlobusEndpoint Class"""

    def __init__(
        self,
        uuid: str,
        endpoint_path: str,
        local_path: Optional[str],
        host_regex: Union[str, Pattern[str]],
    ) -> None:
        """Init GlobusEndpoint

        Args:
            uuid (str): UUID of Globus endpoint.
            endpoint_path (str): path within endpoint to directory to use
                for storing objects.
            local_path (str): local path (as seen by the host filesystem) that
                corresponds to the directory specifed by `endpoint_path`.
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

    def __eq__(self, endpoint):
        """Override __eq__"""
        if (
            self.uuid == endpoint.uuid
            and self.endpoint_path == endpoint.endpoint_path
            and self.local_path == endpoint.local_path
            and self.host_regex == endpoint.host_regex
        ):
            return True
        return False

    def __repr__(self) -> str:
        """String representation of GlobusEndpoint"""
        return (
            f"{self.__class__.__name__}(uuid='{self.uuid}', "
            f"endpoint_path='{self.endpoint_path}', "
            f"local_path='{self.local_path}', "
            f"host_regex='{self.host_regex}')"
        )


class GlobusEndpoints:
    """GlobusEndpoints Class"""

    def __init__(self, *endpoints: List[GlobusEndpoint]) -> None:
        """Init GlobusEndpoints

        Args:
            endpoints (list): list of :class:`GlobusEndpoint <.GlobusEndpoint>`
                instances.
        """
        if len(endpoints) == 1 and isinstance(endpoints[0], list):
            endpoints = endpoints[0]
        if len(endpoints) == 0:
            raise ValueError(
                'GlobusEndpoints must be passed at least one GlobusEndpoint '
                'object'
            )
        self._endpoints = {}
        for endpoint in endpoints:
            if endpoint.uuid in self._endpoints:
                raise ValueError(
                    'Cannot pass multiple GlobusEndpoint objects with the '
                    'same Globus endpoint UUID.'
                )
            self._endpoints[endpoint.uuid] = endpoint

    def __getitem__(self, uuid):
        """Override __getitem__"""
        try:
            return self._endpoints[uuid]
        except KeyError:
            raise KeyError(f"Endpoint with UUID {uuid} does not exist.")

    def __iter__(self):
        """Override __iter__"""

        def _iterator():
            for endpoint in self._endpoints.values():
                yield endpoint

        return _iterator()

    def __len__(self):
        """Override __len__"""
        return len(self._endpoints)

    def __repr__(self):
        """String representation of GlobusEndpoints"""
        s = f"{self.__class__.__name__}(["
        s += ", ".join(str(ep) for ep in self._endpoints.values())
        s += "])"
        return s

    @classmethod
    def from_dict(cls, json_object: Dict[str, Any]) -> GlobusEndpoints:
        """Constructs a GlobusEndpoints object from a dictionary"""
        endpoints = []
        for uuid, params in json_object.items():
            endpoints.append(
                GlobusEndpoint(
                    uuid=uuid,
                    endpoint_path=params['endpoint_path'],
                    local_path=params['local_path'],
                    host_regex=params['host_regex'],
                )
            )
        return GlobusEndpoints(endpoints)

    @classmethod
    def from_json(cls, json_file: str) -> GlobusEndpoints:
        """Constructs a GlobusEndpoints object from a json file"""
        with open(json_file, 'r') as f:
            data = f.read()
        return cls.from_dict(json.loads(data))

    def get_by_host(self, host: str) -> GlobusEndpoint:
        """Get endpoint by host

        Searches the endpoints for a endpoint who's `host_regex` matches `host`.

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


class GlobusFactory(RemoteFactory):
    """Factory for Instances of GlobusStore

    Adds support for asynchronously retrieving objects from a
    :class:`GlobusStore <.GlobusStore>` backend..

    The factory takes the `store_type` and `store_args` parameters that are
    used to reinitialize the backend store if the factory is sent to a remote
    process backend has not already been initialized.
    """

    def __init__(
        self,
        key: str,
        store_name: str,
        store_kwargs: Dict[str, Any] = {},
        *,
        evict: bool = False,
        serialize: bool = True,
        strict: bool = False,
    ) -> None:
        """Init GlobusFactory

        Args:
            key (str): key corresponding to object in store.
            store_name (str): name of store.
            store_kwargs (dict): optional keyword arguments used to
                reinitialize store.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            serialize (bool): if True, object in store is serialized and
                should be deserialized upon retrival (default: True).
            strict (bool): guarentee object produce when this object is called
                is the most recent version of the object associated with the
                key in the store (default: False).
        """
        super(GlobusFactory, self).__init__(
            key,
            GlobusStore,
            store_name,
            store_kwargs,
            evict=evict,
            serialize=serialize,
            strict=strict,
        )


class GlobusStore(RemoteStore):
    """Globus backend class

    The :class:`GlobusStore <.GlobusStore>` is similar to a
    :class:`FileStore <proxystore.store.file.FileStore>` in that objects in the
    store are saved to disk but allows for the transfer of objects between two
    remote file systems. The two directories on the separate file systems are
    kept in sync via Globus transfers. The :class:`GlobusStore <.GlobusStore>`
    is useful when moving data between hosts that have a Globus endpoint but
    may have restrictions that prevent the use of other store backends
    (e.g., ports cannot be opened for using a
    :class:`RedisStore <proxystore.store.redis.RedisStore>`).

    Warning:
        The :class:`GlobusStore <.GlobusStore>` encodes the Globus transfer
        IDs into the keys, thus the keys returned by functions such
        as :func:`set() <set>` will be different.
    """

    def __init__(
        self,
        name: str,
        *,
        endpoints: Union[GlobusEndpoints, List[GlobusEndpoint]],
        polling_interval: int = 1,
        sync_level: Union[int, str] = "mtime",
        timeout: int = 60,
        cache_size: int = 16,
    ) -> None:
        """Init GlobusStore

        Args:
            name (str): name of the store instance.
            endpoints (list, GlobusEndpoints): Globus endpoints to keep
                in sync.
            polling_interval (int): interval in seconds to check if Globus
                tasks have finished.
            sync_level (str, int): Globus transfer sync level.
            timeout (int): timeout in seconds for waiting on Globus tasks.
            cache_size (int): size of local cache (in # of objects). If 0,
                the cache is disabled (default: 16).

        Raise:
            ImportError:
                if `globus_sdk <https://globus-sdk-python.readthedocs.io/en/stable/>`_  # noqa
                or `parsl <https://parsl.readthedocs.io/en/stable/>`_
                is not installed.
            ValueError:
                if `endpoints` is not a list of
                :class:`GlobusEndpoint <.GlobusEndpoint>` or instance of
                :class:`GlobusEndpoints <.GlobusEndpoints>`.
            ValueError:
                if the :code:`len(endpoints) != 2` because
                :class:`GlobusStore <.GlobusStore>` can currently only keep
                two endpoints in sync.
        """
        if import_error is not None:  # pragma: no cover
            raise import_error
        if isinstance(endpoints, GlobusEndpoints):
            self.endpoints = endpoints
        elif isinstance(endpoints, list):
            self.endpoints = GlobusEndpoints(endpoints)
        else:
            raise ValueError(
                "endpoints must be of type GlobusEndpoints or a list of "
                f"GlobusEndpoint. Got {type(endpoints)}."
            )
        if len(endpoints) != 2:
            raise ValueError(
                "ProxyStore only supports two endpoints at a time"
            )
        self.polling_interval = polling_interval
        self.sync_level = sync_level
        self.timeout = timeout

        parsl_globus_auth = globus.get_globus()

        self._transfer_client = globus_sdk.TransferClient(
            authorizer=parsl_globus_auth.authorizer
        )

        super(GlobusStore, self).__init__(name, cache_size=cache_size)

    def _create_key(self, filename: str, task_id: str) -> str:
        """Create key for GlobusStore

        Args:
            filename (str): name of file in Globus.
            task_id (str): Globus task id that should be waited on before
                accessing `filename`.

        Returns:
            key that encodes the `filename` and `task_id`.
        """
        return f"{task_id}:{filename}"

    def _get_filename(self, key: str) -> str:
        """Extract filename from key"""
        return key.split(":")[1]

    def _get_filepath(self, key: str = None, filename: str = None) -> str:
        """Get filepath from key or filename

        Extracts the filename from the key or uses the provided filename
        along with the local path of the local Globus endpoint to create
        the full filepath that can be used to access the file.
        """
        if key is not None and filename is not None:
            raise ValueError("Only one of key or filename may be specified")
        local_endpoint = self._get_local_endpoint()
        os.makedirs(local_endpoint.local_path, exist_ok=True)
        if key is not None:
            filename = self._get_filename(key)
        return os.path.join(local_endpoint.local_path, filename)

    def _get_local_endpoint(self) -> Optional[GlobusEndpoint]:
        """Get endpoint local to current host"""
        return self.endpoints.get_by_host(socket.gethostname())

    def _get_task_id(self, key: str) -> str:
        """Extract task id from key"""
        return key.split(":")[0]

    def _validate_key(self, key: str) -> str:
        """Validate key contains a real Globus task id"""
        if len(key.split(":")) != 2:
            return False
        try:
            self._transfer_client.get_task(self._get_task_id(key))
        except globus_sdk.TransferAPIError as e:
            if e.http_status == 400:
                return False
            raise e
        return True

    def _wait_on_tasks(self, *tasks: List[str]) -> None:
        """Wait on list of Globus tasks"""
        for task in tasks:
            done = self._transfer_client.task_wait(
                task,
                timeout=self.timeout,
                polling_interval=self.polling_interval,
            )
            if not done:
                raise RuntimeError(
                    f"Task {task} did not complete within the " "timeout"
                )

    def _sync_endpoints(self) -> str:
        """Launch Globus Transfer to sync endpoints"""
        src_endpoint = self._get_local_endpoint()
        dst_endpoint = [ep for ep in self.endpoints if ep != src_endpoint]
        assert len(dst_endpoint) == 1
        dst_endpoint = dst_endpoint[0]

        transfer_task = globus_sdk.TransferData(
            self._transfer_client,
            source_endpoint=src_endpoint.uuid,
            destination_endpoint=dst_endpoint.uuid,
            sync_level=self.sync_level,
            delete_destination_extra=True,
        )
        transfer_task['notify_on_succeeded'] = False
        transfer_task['notify_on_failed'] = False
        transfer_task['notify_on_inactive'] = False
        transfer_task.add_item(
            source_path=src_endpoint.endpoint_path,
            destination_path=dst_endpoint.endpoint_path,
            recursive=True,
        )

        tdata = self._transfer_client.submit_transfer(transfer_task)

        return tdata["task_id"]

    def cleanup(self) -> None:
        """Cleanup directories used by ProxyStore in the Globus endpoints

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
            self._wait_on_tasks(tdata["task_id"])

    def evict(self, key: str) -> None:
        """Evict object associated with key from the Globus synced directory

        Args:
            key (str): key corresponding to object in store to evict.
        """
        if not self.exists(key):
            return

        path = self._get_filepath(key)
        os.remove(path)
        self._cache.evict(key)
        self._sync_endpoints()
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')"
        )

    def exists(self, key: str) -> bool:
        """Check if key exists

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        if not self._validate_key(key):
            return False
        self._wait_on_tasks(self._get_task_id(key))
        return os.path.exists(self._get_filepath(key))

    def get_bytes(self, key: str) -> Optional[bytes]:
        """Get serialized object from Globus

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist.
        """
        if not self.exists(key):
            return None

        path = self._get_filepath(key)
        with open(path, 'rb') as f:
            return f.read()

    def get_timestamp(self, key: str) -> float:
        """Get timestamp of most recent object version in the store

        Args:
            key (str): key corresponding to object.

        Returns:
            timestamp (float) representing file modified time (seconds since
            epoch).

        Raises:
            KeyError:
                if `key` does not exist in store.
        """
        if not self.exists(key):
            raise KeyError(
                f"Key='{key}' does not have a corresponding file in the store"
            )
        return os.path.getmtime(self._get_filepath(key))

    def get(
        self,
        key: str,
        *,
        deserialize: bool = True,
        strict: bool = False,
        default: Optional[object] = None,
    ) -> Optional[object]:
        """Return object associated with key

        Args:
            key (str): key corresponding to object.
            deserialize (bool): deserialize object if True. If objects
                are custom serialized, set this as False (default: True).
            strict (bool): guarentee returned object is the most recent
                version (default: False).
            default: optionally provide value to be returned if an object
                associated with the key does not exist (default: None).

        Returns:
            object associated with key or `default` if key does not exist.
        """
        if strict:
            warnings.warn(
                "GlobusStore objects are immutable so setting strict=True "
                "has no effect."
            )
        if self.is_cached(key, strict=strict):
            value = self._cache.get(key)
            logger.debug(
                f"GET key='{key}' FROM {self.__class__.__name__}"
                f"(name='{self.name}'): was_cached=True"
            )
            return value

        value = self.get_bytes(key)
        if value is not None:
            if deserialize:
                value = ps.serialize.deserialize(value)
            if self._cache is not None:
                self._cache.set(key, value)
            logger.debug(
                f"GET key='{key}' FROM {self.__class__.__name__}"
                f"(name='{self.name}'): was_cached=False"
            )
            return value

        logger.debug(
            f"GET key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}'): key did not exist, returned default"
        )
        return default

    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        """Check if object is cached locally

        Args:
            key (str): key corresponding to object.
            strict (bool): guarentee object in cache is most recent version
                (default: False).

        Returns:
            bool
        """
        if self._cache is None:
            return False

        return self._cache.exists(key)

    def set_bytes(self, key: str, data: bytes) -> None:
        """Set serialized object in Globus synced directory with key

        Args:
            key (str): key corresponding to object.
            data (bytes): serialized object.
        """
        if not isinstance(data, bytes):
            raise TypeError(f'data must be of type bytes. Found {type(data)}')
        path = self._get_filepath(filename=key)
        with open(path, 'wb', buffering=0) as f:
            f.write(data)
        # Manually set timestamp on file with nanosecond precision because some
        # filesystems can have low default file modified precisions
        timestamp = time.time_ns()
        os.utime(path, ns=(timestamp, timestamp))

    def set(
        self, obj: Any, *, key: Optional[str] = None, serialize: bool = True
    ) -> str:
        """Set key-object pair in store

        Args:
            obj (object): object to be placed in the store.
            key (str, optional): key to used to name the file in the store.
                If the key is not provided, one will be created. Note the
                actual key that is returned by this function will be different.
            serialize (bool): serialize object if True. If object is already
                custom serialized, set this as False (default: True).

        Returns:
            key (str)
        """
        if serialize:
            obj = ps.serialize.serialize(obj)
        if key is None:
            filename = self.create_key(obj)
        else:
            filename = key

        self.set_bytes(filename, obj)
        tid = self._sync_endpoints()
        key = self._create_key(filename=filename, task_id=tid)
        logger.debug(
            f"SET key='{key}' IN {self.__class__.__name__}"
            f"(name='{self.name}')"
        )
        return key

    def set_batch(
        self,
        objs: Iterable[Any],
        *,
        keys: Optional[Iterable[Optional[str]]] = None,
        serialize: bool = True,
    ) -> List[str]:
        """Set objects in store

        Args:
            objs (Iterable[object]): iterable of objects to be placed in the
                store.
            keys (Iterable[str], optional): keys to use with the objects.
                If the keys are not provided, keys will be created.
            serialize (bool): serialize object if True. If object is already
                custom serialized, set this as False (default: True).

        Returns:
            List of keys (str). Note that some implementations of a store may
            return keys different from the provided keys.

        Raises:
            ValueError:
                if :code:`keys is not None` and :code:`len(objs) != len(keys)`.
        """
        if keys is not None and len(objs) != len(keys):
            raise ValueError(
                f'objs has length {len(objs)} but keys has length {len(keys)}'
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
        tid = self._sync_endpoints()

        keys = []
        for filename in filenames:
            key = self._create_key(filename=filename, task_id=tid)
            logger.debug(
                f"SET key='{key}' IN {self.__class__.__name__}"
                f"(name='{self.name}')"
            )
            keys.append(key)

        return keys

    def proxy(
        self,
        obj: Optional[object] = None,
        *,
        key: Optional[str] = None,
        factory: Factory = GlobusFactory,
        **kwargs,
    ) -> 'proxystore.proxy.Proxy':  # noqa: F821
        """Create a proxy that will resolve to an object in the store

        Args:
            obj (object): object to place in store and return proxy for.
                If an object is not provided, a key must be provided that
                corresponds to an object already in the store (default: None).
            key (str): optional key to associate with `obj` in the store.
                If not provided, a key will be generated (default: None).
            factory (Factory): factory class that will be instantiated
                and passed to the proxy. The factory class should be able
                to correctly resolve the object from this store
                (default: :class:`GlobusFactory <.GlobusFactory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raise:
            ValueError:
                if `key` and `obj` are both `None`.
        """
        if key is None and obj is None:
            raise ValueError('At least one of key or obj must be specified')
        if obj is not None:
            if 'serialize' in kwargs:
                key = self.set(obj, key=key, serialize=kwargs['serialize'])
            else:
                key = self.set(obj, key=key)
        logger.debug(
            f"PROXY key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')"
        )
        return Proxy(
            factory(
                key,
                store_name=self.name,
                store_kwargs={
                    'endpoints': self.endpoints,
                    'polling_interval': self.polling_interval,
                    'sync_level': self.sync_level,
                    'timeout': self.timeout,
                    'cache_size': self.cache_size,
                },
                **kwargs,
            )
        )

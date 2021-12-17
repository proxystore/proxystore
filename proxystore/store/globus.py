"""Globus Endpoint Implementation"""
import os
import re
import socket

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Pattern, Union

import globus_sdk

import proxystore as ps
from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.cache import LRUCache

_default_pool = ThreadPoolExecutor()

GLOBUS_MKDIR_EXISTS_ERROR_CODE = "ExternalError.MkdirFailed.Exists"


class GlobusEndpoint():
    def __init__(
        self,
        uuid: str,
        endpoint_path: str,
        local_path: Optional[str],
        host_regex: Union[str, Pattern[str]],
    ) -> None:
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
        if (
            self.uuid == endpoint.uuid
            and self.endpoint_path == endpoint.endpoint_path
            and self.local_path == endpoint.local_path
            and self.host_regex == endpoint.host_regex
        ):
            return True
        return False


class GlobusEndpoints():
    def __init__(self, *endpoints: List[GlobusEndpoint]) -> None:
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
        try:
            return self._endpoints[uuid]
        except KeyError as e:
            raise KeyError(
                f"Endpoint with UUID {uuid} does not exist."
            )

    def __iter__(self):
        def _iterator():
            for endpoint in self._endpoints.values():
                yield endpoint
        return _iterator()

    def __len__(self):
        return len(self._endpoints)

    def get_by_host(self, host):
        for endpoint in self._endpoints.values():
            if re.fullmatch(endpoint.host_regex, host) is not None:
                return endpoint
        raise ValueError(f'Cannot find endpoint matching host {host}')


class GlobusFactory(Factory):
    def __init__(
        self,
        key: str,
        name: str,
        endpoints: Union[GlobusEndpoints, List[GlobusEndpoint]],
        sync_level: Union[str, int],
        *,
        evict: bool = False,
        serialize: bool = True,
        strict: bool = False,
        **kwargs: Dict[str, Any],
    ) -> None:
        self.key = key
        self.name = name
        self.endpoints = endpoints
        self.sync_level = sync_level
        self.evict = evict
        self.serialize = serialize
        self.strict = strict
        self._kwargs = kwargs
        self._obj_future = None
    
    def __getnewargs_ex__(self):
        """Helper method for pickling"""
        return (self.key, self.name, self.endpoints, self.sync_level), {
            'evict': self.evict,
            'serialize': self.serialize,
            'strict': self.strict,
            **self._kwargs,
        }

    def resolve(self) -> None:
        """Get object associated with key from Globus endpoint"""
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        store = ps.store.get_store(self.name)
        if store is None:
            store = ps.store.init_store(
                ps.store.STORES.GLOBUS,
                self.name,
                endpoints=self.endpoints,
                sync_level=self.sync_level,
                **self._kwargs,
            )

        obj = store.get(
            self.key, deserialize=self.serialize, strict=self.strict
        )
        if self.evict:
            store.evict(self.key)
        return obj

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from Globus endpoint"""
        store = ps.store.get_store(self.name)
        if store is None:
            store = ps.store.init_store(
                ps.store.STORES.GLOBUS,
                self.name,
                endpoints=self.endpoints,
                sync_level=self.sync_level,
                **self._kwargs,
            )

        # If the value is locally cached by the value server, starting up
        # a separate thread to retrieve a cached value will be slower than
        # just getting the value from the cache
        if store.is_cached(self.key, strict=self.strict):
            return

        self._obj_future = _default_pool.submit(
            store.get,
            self.key,
            deserialize=self.serialize,
            strict=self.strict,
        )


class GlobusStore(Store):
    """Globus backend class"""

    def __init__(
        self,
        name: str,
        endpoints: Union[GlobusEndpoints, List[GlobusEndpoint]],
        sync_level: Union[int, str] = "mtime",
        cache_size: int = 16,
    ) -> None:
        """Init GlobusStore"""
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
        if cache_size < 0:
            raise ValueError('Cache size cannot be negative')
        self.name = name
        self.sync_level = sync_level
        self.cache_size = cache_size
        self._cache = LRUCache(cache_size) if cache_size > 0 else None

        from parsl.data_provider.globus import get_globus

        parsl_globus_auth = get_globus()

        self._transfer_client = globus_sdk.TransferClient(
            authorizer=parsl_globus_auth.authorizer
        )

    def _create_key(self, filename: str, task_id: str) -> str:
        return f"{task_id}:{filename}"
    
    def _get_filename(self, key: str) -> str:
        return key.split(":")[1]

    def _get_filepath(self, key: str = None, filename: str = None) -> str:
        if key is not None and filename is not None:
            raise ValueError("Only one of key or filename may be specified")
        local_endpoint = self._get_local_endpoint()
        os.makedirs(local_endpoint.local_path, exist_ok=True)
        if key is not None:
            filename = self._get_filename(key)
        return os.path.join(local_endpoint.local_path, filename)

    def _get_local_endpoint(self) -> Optional[GlobusEndpoint]:
        return self.endpoints.get_by_host(socket.gethostname())

    def _get_task_id(self, key: str) -> str:
        return key.split(":")[0]

    def _validate_key(self, key: str) -> str:
        try:
            self._transfer_client.get_task(self._get_task_id(key))
        except globus_sdk.TransferAPIError as e:
            if e.http_status == 400:
                return False
            raise e
        return True

    def _wait_on_tasks(
        self, *tasks: List[str], timeout: int = 60, polling_interval: int = 1
    ) -> None:
        for task in tasks:
            done = self._transfer_client.task_wait(
                task, 
                timeout=timeout,
                polling_interval=polling_interval
            )
            if not done:
                raise RuntimeError(
                    f"Task {task} did not complete within the "
                    "timeout"
                )

    def _sync_endpoints(self) -> str:
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
        if not self.exists(key):
            return

        path = self._get_filepath(key)
        if os.path.exists(path):
            os.remove(path)
        self._cache.evict(key)

        self._sync_endpoints()

    def exists(self, key: str) -> bool:
        if not self._validate_key(key):
            return False
        self._wait_on_tasks(self._get_task_id(key))
        return os.path.exists(self._get_filepath(key))

    def get(
        self,
        key: str,
        *,
        deserialize: bool = True,
        strict: bool = False,
        default: Optional[object] = None,
    ) -> Optional[object]:
        if self.is_cached(key, strict=strict):
            return self._cache.get(key)

        if not self.exists(key):
            return default

        path = self._get_filepath(key)
        if not os.path.exists(path):
            return default
        
        with open(path, 'r') as f:
            value = f.read()

        if deserialize:
            value = ps.serialize.deserialize(value)
        if self._cache is not None:
            self._cache.set(key, value)
        return value

    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        if self._cache is None:
            return False

        return self._cache.exists(key)

    def set(
        self, obj: Any, *, key: Optional[str] = None, serialize: bool = True
    ) -> str:
        if serialize: 
            obj = ps.serialize.serialize(obj)

        if key is None:
            filename = self.create_key(obj)
        else:
            filename = key
        path = self._get_filepath(filename=filename)
        with open(path, 'w') as f:
            f.write(obj)

        tid = self._sync_endpoints()
        return self._create_key(filename=filename, task_id=tid)

    def proxy(
        self,
        obj: Optional[object] = None,
        *,
        key: Optional[str] = None,
        factory: Factory = GlobusFactory,
        **kwargs,
    ) -> 'proxystore.proxy.Proxy':  # noqa: F821
        if (
            (key is None and obj is None)
            or (key is not None and obj is not None)
        ):
            raise ValueError('Exactly one of obj and key must be provided')
        if obj is not None:
            if 'serialize' in kwargs:
                key = self.set(obj, serialize=kwargs['serialize'])
            else:
                key = self.set(obj)
        
        return Proxy(
            factory(
                key,
                self.name,
                self.endpoints,
                self.sync_level,
                cache_size=self.cache_size,
                **kwargs,
            )
        )

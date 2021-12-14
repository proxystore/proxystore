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
from proxystore.store.base import RemoteStore

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
    def __init__(self, endpoints: List[GlobusEndpoint]) -> None:
        self._endpoints = {}
        for endpoint in endpoints:
            self._endpoints[endpoint.uuid] = endpoint

    def __getitem__(self, key):
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

    def get_by_host(self, host):
        for endpoint in self._endpoints.values():
            if re.fullmatch(endpoint.host_regex, host) is not None:
                return endpoint
        # TODO(gpauloski): raise exception instead?
        return None


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


class GlobusStore(RemoteStore):
    """Globus backend class"""

    def __init__(
        self,
        name: str,
        endpoints: Union[GlobusEndpoints, List[GlobusEndpoint]],
        sync_level: Union[int, str] = "mtime",
        cache_size: int = 16,
    ) -> None:
        """Init GlobusStore"""
        # TODO(gpauloski): set sync level
        # TODO(gpauloski): change to recursive sync on directory
        if isinstance(endpoints, GlobusEndpoints):
            self.endpoints = endpoints
        elif isinstance(endpoints, list):
            self.endpoints = GlobusEndpoints(endpoints)
        else:
            raise ValueError(
                "endpoints must be of type GlobusEndpoints or a list of "
                f"GlobusEndpoint. Got {type(endpoints)}."
            )
        self.sync_level = sync_level
        
        super(GlobusStore, self).__init__(name, cache_size=cache_size)


        from parsl.data_provider.globus import get_globus

        parsl_globus_auth = get_globus()

        self._transfer_client = globus_sdk.TransferClient(
            authorizer=parsl_globus_auth.authorizer
        )

        # Make local directories in each endpoint
        for endpoint in self.endpoints:
            try:
                response = self._transfer_client.operation_mkdir(
                    endpoint.uuid, endpoint.endpoint_path
                )
            except globus_sdk.TransferAPIError as e:
                if e.code != GLOBUS_MKDIR_EXISTS_ERROR_CODE:
                    print(
                        f'Failed to create directory {endpoint.endpoint_path} '
                        f'at endpoint {endpoint.uuid}: {e.message}'
                    )

    def _get_local_endpoint(self) -> Optional[GlobusEndpoint]:
        try:
            return self.endpoints.get_by_host(socket.gethostname())
        except KeyError as e:
            return None

    def _wait_on_tasks(
        self, *tasks: List[str], timeout: int = 10, polling_interval: int = 1
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

    def _sync_endpoints(
        self,
        src_endpoint: GlobusEndpoint,
        dst_endpoints: Optional[List[GlobusEndpoint]] = None,
    ) -> None:
        # TODO(gpauloski): if we keep this structure, should support single
        # dst_endpoint and a GlobusEndpoints object
        if dst_endpoints is None:
            dst_endpoints = [
                endpoint for endpoint in self.endpoints
                if endpoint != src_endpoint
            ]

        for dst_endpoint in dst_endpoints:
            if dst_endpoint == src_endpoint:
                continue
            transfer_task = globus_sdk.TransferData(
                self._transfer_client,
                source_endpoint=src_endpoint.uuid,
                destination_endpoint=dst_endpoint.uuid,
                sync_level=self.sync_level,
                delete_destination_extra=True,
                #additional_fields={
                #    'notify_on_succeeded': False,
                #    'notify_on_failed': False,
                #    'notify_on_inactive': False,
                #}
            )
            transfer_task['notify_on_succeeded'] = False
            transfer_task['notify_on_failed'] = False
            transfer_task['notify_on_inactive'] = False
            transfer_task.add_item(
                source_path=src_endpoint.endpoint_path,
                destination_path=dst_endpoint.endpoint_path,
                recursive=True,
            )
            self._transfer_client.submit_transfer(transfer_task)

    def cleanup(self) -> None:
        for endpoint in self.endpoints:
            delete_task = globus_sdk.DeleteData(
                self._transfer_client,
                endpoint=endpoint.uuid,
                recursive=True,
                additional_fields={
                    'notify_on_succeeded': False,
                    'notify_on_failed': False,
                    'notify_on_inactive': False,
                }
            )
            delete_task.add_item(endpoint.endpoint_path)
            self._transfer_client.submit_delete(delete_task)

    def create_key(self) -> None:
        return self._transfer_client.get_submission_id()

    def evict(self, key: str) -> None:
        # Delete in local endpoint
        local_endpoint = self._get_local_endpoint()
        if local_endpoint is not None:
            path = os.path.join(local_endpoint.local_path, key)
            if os.path.exists(path):
                os.remove(path)

        # Delete on remote endpoints
        self._sync_endpoints(key, local_endpoint)

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def get_str(self, key: str) -> Optional[str]:
        local_endpoint = self._get_local_endpoint()
        #self._wait_on_tasks(key)
        path = os.path.join(local_endpoint.local_path, key)

        # TODO(gpauloski): This is bad
        import time
        slept = 0
        while not os.path.exists(path):
            slept += 0.1
            time.sleep(0.1)
            if slept == 5:
                break

        if os.path.exists(path):
            with open(path, 'r') as f:
                data = f.read()
                return data
        return None

    def set_str(self, key: str, data: str) -> None:
        # TODO(gpauloski): we could cancel tasks instead because
        # data will be overwritten anyways
        local_endpoint = self._get_local_endpoint()
        #self._wait_on_tasks(key)
        path = os.path.join(local_endpoint.local_path, key)
        with open(path, 'w') as f:
            f.write(data)

        self._sync_endpoints(local_endpoint)

    def proxy(
        self,
        obj: Optional[object] = None,
        key: Optional[str] = None,
        *,
        factory: Factory = GlobusFactory,
        **kwargs,
    ) -> 'proxystore.proxy.Proxy':  # noqa: F821
        if key is None and obj is None:
            raise ValueError('At least one of key or obj must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            if 'serialize' in kwargs:
                self.set(key, obj, serialize=kwargs['serialize'])
            else:
                self.set(key, obj)
        else:
            # TODO(gpauloski)
            raise NotImplementedError
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

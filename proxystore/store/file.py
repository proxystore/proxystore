"""FileStore Implementation"""
import logging
import os
import shutil
import time

from typing import Any, Dict, Optional

from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.store.remote import RemoteFactory, RemoteStore

logger = logging.getLogger(__name__)


class FileFactory(RemoteFactory):
    """Factory for Instances of FileStore

    Adds support for asynchronously retrieving objects from a
    :class:`FileStore <.FileStore>` backend and optional, strict guarentees
    on object versions.

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
        """Init FileFactory

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
        super(FileFactory, self).__init__(
            key,
            FileStore,
            store_name,
            store_kwargs,
            evict=evict,
            serialize=serialize,
            strict=strict,
        )


class FileStore(RemoteStore):
    """File backend class"""

    def __init__(
        self,
        name: str,
        *,
        store_dir: str,
        cache_size: int = 16,
    ) -> None:
        """Init FileStore

        Args:
            name (str): name of the store instance.
            store_dir (str): path to directory
            cache_size (int): size of local cache (in # of objects). If 0,
                the cache is disabled (default: 16).
        """
        self.store_dir = store_dir

        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir, exist_ok=True)

        super(FileStore, self).__init__(name, cache_size=cache_size)

    def cleanup(self) -> None:
        """Cleanup all files associated with the file system store

        Warning:
            Will delete the `store_dir` directory.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.
        """
        shutil.rmtree(self.store_dir)

    def evict(self, key: str) -> None:
        """Remove the object associated with key from the file system store

        Args:
            key (str): key corresponding to object in store to evict.
        """
        path = os.path.join(self.store_dir, key)
        if os.path.exists(path):
            os.remove(path)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')"
        )

    def exists(self, key: str) -> bool:
        """Check if key exists in file system store

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        path = os.path.join(self.store_dir, key)
        return os.path.exists(path)

    def get_bytes(self, key: str) -> Optional[bytes]:
        """Get serialized object from file system

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist.
        """
        path = os.path.join(self.store_dir, key)
        if os.path.exists(path):
            with open(path, 'rb') as f:
                data = f.read()
                return data
        return None

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
        return os.path.getmtime(os.path.join(self.store_dir, key))

    def set_bytes(self, key: str, data: bytes) -> None:
        """Write serialized object to file system with key

        Args:
            key (str): key corresponding to object.
            data (bytes): serialized object.
        """
        if not isinstance(data, bytes):
            raise TypeError(f'data must be of type bytes. Found {type(data)}')
        path = os.path.join(self.store_dir, key)
        with open(path, 'wb', buffering=0) as f:
            f.write(data)
        # Manually set timestamp on file with nanosecond precision because some
        # filesystems can have low default file modified precisions
        timestamp = time.time_ns()
        os.utime(path, ns=(timestamp, timestamp))

    def proxy(
        self,
        obj: Optional[object] = None,
        *,
        key: Optional[str] = None,
        factory: Factory = FileFactory,
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
                (default: :class:`FileFactory <.FileFactory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
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
                    'store_dir': self.store_dir,
                    'cache_size': self.cache_size,
                },
                **kwargs,
            )
        )

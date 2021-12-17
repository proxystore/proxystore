"""FileStore Implementation"""
import os
import shutil

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

import proxystore as ps
from proxystore.factory import Factory
from proxystore.proxy import Proxy
from proxystore.store.base import RemoteStore

_default_pool = ThreadPoolExecutor()


class FileFactory(Factory):
    """Factory for FileStore

    Adds support for asynchronously retrieving objects stored on a file system
    via the :class:`FileStore <.FileStore>` backend.

    The :class:`FileFactory <.FileFactory>` stores the path to the
    directory on the file system where objects are written such that a new
    instance of :class:`FileStore <.FileStore>` can be created
    if this factory is passed to a different process or machine.
    """

    def __init__(
        self,
        key: str,
        name: str,
        store_dir: str,
        *,
        evict: bool = False,
        serialize: bool = True,
        strict: bool = False,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Init FileFactory

        Args:
            key (str): key corresponding to object in store.
            name (str): name of store to retrieve object from.
            store_dir (str): path to directory where objects are written.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            serialize (bool): if True, object in store is serialized and
                should be deserialized upon retrival (default: True).
            strict (bool): guarentee object produce when this object is called
                is the most recent version of the object associated with the
                key in the store (default: False).
            kwargs (dict): additional keyword arguments to pass to
                :class:`FileStore <.FileStore>` to rebuild the
                instance.
        """
        self.key = key
        self.name = name
        self.store_dir = store_dir
        self.evict = evict
        self.serialize = serialize
        self.strict = strict
        self._kwargs = kwargs
        self._obj_future = None

    def __getnewargs_ex__(self):
        """Helper method for pickling"""
        return (self.key, self.name, self.store_dir), {
            'evict': self.evict,
            'serialize': self.serialize,
            'strict': self.strict,
            **self._kwargs,
        }

    def resolve(self) -> None:
        """Get object associated with key from file system"""
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        store = ps.store.get_store(self.name)
        if store is None:
            store = ps.store.init_store(
                ps.store.STORES.FILE,
                self.name,
                store_dir=self.store_dir,
                **self._kwargs,
            )

        obj = store.get(
            self.key, deserialize=self.serialize, strict=self.strict
        )
        if self.evict:
            store.evict(self.key)
        return obj

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from file system"""
        store = ps.store.get_store(self.name)
        if store is None:
            store = ps.store.init_store(
                ps.store.STORES.FILE,
                self.name,
                store_dir=self.store_dir,
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


class FileStore(RemoteStore):
    """File backend class"""

    def __init__(
        self,
        name: str,
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

    def exists(self, key: str) -> bool:
        """Check if key exists in file system store

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        path = os.path.join(self.store_dir, key)
        return os.path.exists(path)

    def get_str(self, key: str) -> Optional[str]:
        """Get serialized object from file system

        Args:
            key (str): key corresponding to object.

        Returns:
            serialized object or `None` if it does not exist.
        """
        path = os.path.join(self.store_dir, key)
        if os.path.exists(path):
            # TODO(gpauloski): writing hex escape characters
            with open(path, 'r') as f:
                data = f.read()
                return data  # .hex()
        return None

    def set_str(self, key: str, data: str) -> None:
        """Write serialized object to file system with key

        Args:
            key (str): key corresponding to object.
            data (str): serialized object.
        """
        path = os.path.join(self.store_dir, key)
        with open(path, 'w') as f:
            # TODO(gpauloski): writing hex escape characters
            f.write(data)  # bytes.fromhex(data))

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
            ValueError:
                if `obj` is None and `key` does not exist in the store.
        """
        if key is None and obj is None:
            raise ValueError('At least one of key or obj must be specified')
        if key is None:
            key = ps.utils.create_key(obj)
        if obj is not None:
            if 'serialize' in kwargs:
                self.set(obj, key=key, serialize=kwargs['serialize'])
            else:
                self.set(obj, key=key)
        elif not self.exists(key):
            raise ValueError(
                f'An object with key {key} does not exist in the store'
            )
        return Proxy(
            factory(
                key,
                self.name,
                self.store_dir,
                cache_size=self.cache_size,
                **kwargs,
            )
        )

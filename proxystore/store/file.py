"""FileStore Implementation."""
from __future__ import annotations

import logging
import os
import shutil
from typing import Any
from typing import NamedTuple

import proxystore.utils as utils
from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class FileStoreKey(NamedTuple):
    """Key to objects in a FileStore."""

    filename: str
    """Unique object filename."""


class FileStore(Store[FileStoreKey]):
    """File backend class."""

    def __init__(
        self,
        name: str,
        *,
        store_dir: str,
        cache_size: int = 16,
        stats: bool = False,
    ) -> None:
        """Init FileStore.

        Args:
            name (str): name of the store instance.
            store_dir (str): path to directory to store data in. Note this
                directory will be deleted upon closing the store.
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """
        self.store_dir = os.path.abspath(store_dir)

        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir, exist_ok=True)

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'store_dir': self.store_dir},
        )

    def close(self) -> None:
        """Cleanup all files associated with the file system store.

        Warning:
            Will delete the `store_dir` directory.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.
        """
        shutil.rmtree(self.store_dir)

    def create_key(self, obj: Any) -> FileStoreKey:
        return FileStoreKey(filename=utils.create_key(obj))

    def evict(self, key: FileStoreKey) -> None:
        path = os.path.join(self.store_dir, key.filename)
        if os.path.exists(path):
            os.remove(path)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: FileStoreKey) -> bool:
        path = os.path.join(self.store_dir, key.filename)
        return os.path.exists(path)

    def get_bytes(self, key: FileStoreKey) -> bytes | None:
        path = os.path.join(self.store_dir, key.filename)
        if os.path.exists(path):
            with open(path, 'rb') as f:
                data = f.read()
                return data
        return None

    def set_bytes(self, key: FileStoreKey, data: bytes) -> None:
        path = os.path.join(self.store_dir, key.filename)
        with open(path, 'wb', buffering=0) as f:
            f.write(data)

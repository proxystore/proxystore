"""FileStore Implementation."""
from __future__ import annotations

import logging
import os
import shutil
import time

from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class FileStore(Store):
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
        self.store_dir = store_dir

        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir, exist_ok=True)

        super().__init__(
            name,
            cache_size=cache_size,
            stats=stats,
            kwargs={'store_dir': store_dir},
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

    def evict(self, key: str) -> None:
        path = os.path.join(self.store_dir, key)
        if os.path.exists(path):
            os.remove(path)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        path = os.path.join(self.store_dir, key)
        return os.path.exists(path)

    def get_bytes(self, key: str) -> bytes | None:
        path = os.path.join(self.store_dir, key)
        if os.path.exists(path):
            with open(path, 'rb') as f:
                data = f.read()
                return data
        return None

    def get_timestamp(self, key: str) -> float:
        if not self.exists(key):
            raise KeyError(
                f"Key='{key}' does not have a corresponding file in the store",
            )
        return os.path.getmtime(os.path.join(self.store_dir, key))

    def set_bytes(self, key: str, data: bytes) -> None:
        """Write serialized object to file system with key.

        Args:
            key (str): key corresponding to object.
            data (bytes): serialized object.
        """
        path = os.path.join(self.store_dir, key)
        with open(path, 'wb', buffering=0) as f:
            f.write(data)
        # Manually set timestamp on file with nanosecond precision because some
        # filesystems can have low default file modified precisions
        timestamp = time.time_ns()
        os.utime(path, ns=(timestamp, timestamp))

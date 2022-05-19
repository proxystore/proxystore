"""FileStore Implementation."""
from __future__ import annotations

import logging
import os
import shutil
import time
from typing import Any

from proxystore.store.base import Store

logger = logging.getLogger(__name__)


class FileStore(Store):
    """File backend class."""

    def __init__(
        self,
        name: str,
        *,
        store_dir: str,
        **kwargs: Any,
    ) -> None:
        """Init FileStore.

        Args:
            name (str): name of the store instance.
            store_dir (str): path to directory
            kwargs (dict): additional keyword arguments to pass to
                :class:`Store <proxystore.store.base.Store>`.
        """
        self.store_dir = store_dir

        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir, exist_ok=True)

        super().__init__(name, **kwargs)

    def _kwargs(
        self,
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Helper for handling inheritance with kwargs property.

        Args:
            kwargs (optional, dict): dict to use as return object. If None,
                a new dict will be created.
        """
        if kwargs is None:
            kwargs = {}
        kwargs.update({'store_dir': self.store_dir})
        return super()._kwargs(kwargs)

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
        """Remove the object associated with key from the file system store.

        Args:
            key (str): key corresponding to object in store to evict.
        """
        path = os.path.join(self.store_dir, key)
        if os.path.exists(path):
            os.remove(path)
        self._cache.evict(key)
        logger.debug(
            f"EVICT key='{key}' FROM {self.__class__.__name__}"
            f"(name='{self.name}')",
        )

    def exists(self, key: str) -> bool:
        """Check if key exists in file system store.

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        path = os.path.join(self.store_dir, key)
        return os.path.exists(path)

    def get_bytes(self, key: str) -> bytes | None:
        """Get serialized object from file system.

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
        """Get timestamp of most recent object version in the store.

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
                f"Key='{key}' does not have a corresponding file in the store",
            )
        return os.path.getmtime(os.path.join(self.store_dir, key))

    def set_bytes(self, key: str, data: bytes) -> None:
        """Write serialized object to file system with key.

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

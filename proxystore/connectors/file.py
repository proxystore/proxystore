"""File system connector implementation."""
from __future__ import annotations

import logging
import os
import shutil
import sys
import uuid
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

logger = logging.getLogger(__name__)


class FileKey(NamedTuple):
    """Key to objects in a file system directory.

    Attributes:
        filename: Unique object filename.
    """

    filename: str


class FileConnector:
    """Connector to shared file system.

    Args:
        store_dir: Path to directory to store data in. Note this
            directory will be deleted upon closing the store.
        clear: Clear all objects on
            [`close()`][proxystore.connectors.file.FileConnector] by removing
            `store_dir`.
    """

    def __init__(self, store_dir: str, clear: bool = True) -> None:
        self.store_dir = os.path.abspath(store_dir)
        self.clear = clear

        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir, exist_ok=True)

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
        return f'{self.__class__.__name__}(directory={self.store_dir})'

    def close(self, clear: bool | None = None) -> None:
        """Close the connector and clean up.

        Warning:
            This will delete the `store_dir` directory by default.

        Warning:
            This method should only be called at the end of the program
            when the connector will no longer be used, for example once all
            proxies have been resolved.

        Args:
            clear: Remove the store directory. Overrides the default
                value of `clear` provided when the
                [`FileConnector`][proxystore.connectors.file.FileConnector]
                was instantiated.
        """
        clear = self.clear if clear is None else clear
        if clear and os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir)

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {'store_dir': self.store_dir, 'clear': self.clear}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> FileConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: FileKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        path = os.path.join(self.store_dir, key.filename)
        if os.path.exists(path):
            os.remove(path)

    def exists(self, key: FileKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        path = os.path.join(self.store_dir, key.filename)
        return os.path.exists(path)

    def get(self, key: FileKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        path = os.path.join(self.store_dir, key.filename)
        if os.path.exists(path):
            with open(path, 'rb') as f:
                data = f.read()
                return data
        return None

    def get_batch(self, keys: Sequence[FileKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def put(self, obj: bytes) -> FileKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = FileKey(filename=str(uuid.uuid4()))

        path = os.path.join(self.store_dir, key.filename)
        with open(path, 'wb', buffering=0) as f:
            f.write(obj)

        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[FileKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]

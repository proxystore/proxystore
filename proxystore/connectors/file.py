"""File system connector implementation."""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import uuid
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Self

from proxystore._compat import init_kwargs
from proxystore.serialize import BytesLike

logger = logging.getLogger(__name__)


class FileKey(NamedTuple):
    """Key to objects in a file system directory.

    Attributes:
        filename: Unique object filename.
    """

    filename: str


class FileConnector:
    """Connector to shared file system.

    This connector writes objects to unique files within `store_dir`. Each
    object is written to a hidden temporary file in `store_dir` which is
    then renamed, so an object's file only exists once it is completely
    written.

    Args:
        store_dir: Path to directory to store data in. This directory is
            removed when the connector is closed with `clear=True` (the
            default), such as by the
            [`owner`][proxystore.store.base.Store.owner] store.
        clear: Clear all objects on
            [`close()`][proxystore.connectors.file.FileConnector.close] by
            removing `store_dir`.
        buffering: Buffering policy used with [`open()`][open].
        dir_mode: Permissions of `store_dir` if the connector creates it.
            The default only gives the current user access. An existing
            `store_dir` keeps its permissions, and parent directories are
            created with the default permissions.
        file_mode: Permissions of the object files. The default only gives
            the current user access.

    Note:
        `dir_mode` and `file_mode` are applied exactly and are not limited
        by the [umask](https://en.wikipedia.org/wiki/Umask){target=_blank},
        so, e.g., `#!python dir_mode=0o770, file_mode=0o660` shares the
        objects with the group of `store_dir`.
    """

    def __init__(
        self,
        store_dir: str | os.PathLike[str],
        *,
        clear: bool = True,
        buffering: int = -1,
        dir_mode: int = 0o700,
        file_mode: int = 0o600,
    ) -> None:
        self.store_dir = os.path.abspath(store_dir)
        self.clear = clear
        self.buffering = buffering
        self.dir_mode = dir_mode
        self.file_mode = file_mode

        if not os.path.exists(self.store_dir):
            os.makedirs(os.path.dirname(self.store_dir), exist_ok=True)
            try:
                os.mkdir(self.store_dir, mode=self.dir_mode)
            except FileExistsError:
                # Another process created the directory first.
                pass
            else:
                os.chmod(self.store_dir, self.dir_mode)

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

    def close(self, *, clear: bool | None = None) -> bool:
        """Close the connector and clean up.

        Warning:
            This will delete the `store_dir` directory by default.

        Note:
            A [`Store`][proxystore.store.base.Store] only clears its
            connector when closed if the store is the
            [`owner`][proxystore.store.base.Store.owner]. Stores created
            implicitly, such as when resolving a proxy, never clear their
            connector.

        Warning:
            This method should only be called at the end of the program
            when the connector will no longer be used, for example once all
            proxies have been resolved.

        Args:
            clear: Remove the store directory. Overrides the default
                value of `clear` provided when the
                [`FileConnector`][proxystore.connectors.file.FileConnector]
                was instantiated.

        Returns:
            If the store directory was removed.
        """
        clear = self.clear if clear is None else clear
        if clear and os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir, ignore_errors=True)
        return clear

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            'store_dir': self.store_dir,
            'clear': self.clear,
            'buffering': self.buffering,
            'dir_mode': self.dir_mode,
            'file_mode': self.file_mode,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> FileConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**init_kwargs(cls, config))

    def evict(self, key: FileKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        # The file may already have been removed by a concurrent evict().
        with contextlib.suppress(FileNotFoundError):
            os.remove(self._path(key))

    def exists(self, key: FileKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        return os.path.exists(self._path(key))

    def get(self, key: FileKey) -> BytesLike | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        try:
            with open(self._path(key), 'rb', buffering=self.buffering) as f:
                return f.read()
        except FileNotFoundError:
            return None

    def get_batch(self, keys: Sequence[FileKey]) -> list[BytesLike | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def new_key(self, obj: BytesLike | None = None) -> FileKey:
        """Create a new key.

        Args:
            obj: Optional object which the key will be associated with.
                Ignored by this implementation.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.file.FileConnector.set] \
            has been called on the key.
        """
        return FileKey(filename=str(uuid.uuid4()))

    def put(self, obj: BytesLike) -> FileKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = FileKey(filename=str(uuid.uuid4()))
        self.set(key, obj)
        return key

    def put_batch(self, objs: Sequence[BytesLike]) -> list[FileKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]

    def set(self, key: FileKey, obj: BytesLike) -> None:
        """Set the object associated with a key.

        Note:
            The [`Connector`][proxystore.connectors.protocols.Connector]
            provides write-once, read-many semantics. Thus,
            [`set()`][proxystore.connectors.file.FileConnector.set]
            should only be called once per key, otherwise unexpected behavior
            can occur.

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        # Write to a unique temporary file and rename it so readers never
        # see a partially written file, and concurrent writers of the same
        # key do not write to the same file.
        tmp = os.path.join(
            self.store_dir,
            f'.{key.filename}.{uuid.uuid4().hex}.tmp',
        )
        try:
            with open(
                tmp,
                'xb',
                buffering=self.buffering,
                opener=self._opener,
            ) as f:
                os.fchmod(f.fileno(), self.file_mode)
                f.write(obj)
            os.replace(tmp, self._path(key))
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.remove(tmp)
            raise

    def _opener(self, path: str, flags: int) -> int:
        return os.open(path, flags, self.file_mode)

    def _path(self, key: FileKey) -> str:
        return os.path.join(self.store_dir, key.filename)

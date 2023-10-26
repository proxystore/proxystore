"""In-process local storage connector implementation."""
from __future__ import annotations

import logging
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


class LocalKey(NamedTuple):
    """Key to objects store in a `LocalConnector`.

    Attributes:
        id: Unique object ID.
    """

    id: str


class LocalConnector:
    """Connector that store objects in the local process's memory.

    Warning:
        This connector exists primarily for testing purposes.

    Args:
        store_dict: Dictionary to store data in. If not specified,
            a new empty dict will be generated.
    """

    def __init__(
        self,
        store_dict: dict[LocalKey, bytes] | None = None,
    ) -> None:
        self._store: dict[LocalKey, bytes] = {}
        if store_dict is not None:
            self._store = store_dict

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
        return f'{self.__class__.__name__}()'

    def close(self) -> None:
        """Close the connector and clean up."""
        pass

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {'store_dict': self._store}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> LocalConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        return cls(**config)

    def evict(self, key: LocalKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        if key in self._store:
            del self._store[key]

    def exists(self, key: LocalKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        return key in self._store

    def get(self, key: LocalKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        return self._store.get(key, None)

    def get_batch(self, keys: Sequence[LocalKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def new_key(self, obj: bytes | None = None) -> LocalKey:
        """Create a new key.

        Args:
            obj: Optional object which the key will be associated with.
                Ignored in this implementation.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.local.LocalConnector.set] \
            has been called on the key.
        """
        return LocalKey(str(uuid.uuid4()))

    def put(self, obj: bytes) -> LocalKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        key = LocalKey(str(uuid.uuid4()))
        self._store[key] = obj
        return key

    def put_batch(self, objs: Sequence[bytes]) -> list[LocalKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        return [self.put(obj) for obj in objs]

    def set(self, key: LocalKey, obj: bytes) -> None:
        """Set the object associated with a key.

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        self._store[key] = obj

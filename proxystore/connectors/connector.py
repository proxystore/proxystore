"""Connector protocol."""
from __future__ import annotations

from typing import Any
from typing import NamedTuple
from typing import Protocol
from typing import runtime_checkable
from typing import Sequence
from typing import TypeVar

KeyT = TypeVar('KeyT', bound=NamedTuple)


@runtime_checkable
class Connector(Protocol[KeyT]):
    """Connector protocol for interfacing with external object storage.

    The Connector protocol defines the interface for interacting with a
    byte-level object store.
    """

    def close(self) -> None:
        """Close the connector and clean up."""
        ...

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.

        Returns:
            Connector configuration.
        """
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Connector[Any]:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.

        Returns:
            Connector instance.
        """
        ...

    def evict(self, key: KeyT) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        ...

    def exists(self, key: KeyT) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        ...

    def get(self, key: KeyT) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        ...

    def get_batch(self, keys: Sequence[KeyT]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        ...

    def put(self, obj: bytes, key_id: KeyT | None = None) -> KeyT:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        ...

    def put_batch(
        self,
        objs: Sequence[bytes],
        key_id: KeyT | None = None,
    ) -> list[KeyT]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        ...

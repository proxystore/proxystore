"""Connector protocol."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from typing import NamedTuple
from typing import Protocol
from typing import runtime_checkable
from typing import Self
from typing import TypeVar

from proxystore.serialize import BytesLike

KeyT = TypeVar('KeyT', bound=NamedTuple)


@runtime_checkable
class Connector(Protocol[KeyT]):
    """Connector protocol for interfacing with external object storage.

    The Connector protocol defines the interface for interacting with
    a byte-level object store.

    Note:
        Keys and connector configurations are included in pickled proxies,
        which may be exchanged between processes using different versions
        of a connector. Keys are pickled by the import path of the key type,
        so the key type and its fields should not change. Configuration keys
        can be added, but
        [`from_config()`][proxystore.connectors.protocols.Connector.from_config]
        should ignore unknown keys from newer versions, ideally with a
        [`VersionMismatchWarning`][proxystore.warnings.VersionMismatchWarning],
        as the builtin connectors do.

    Note:
        Implementations must be thread-safe. A
        [`Store`][proxystore.store.base.Store] may call the methods of one
        connector from multiple threads at the same time, including with
        the same key (e.g., two threads calling
        [`evict()`][proxystore.connectors.protocols.Connector.evict] while
        another calls
        [`get()`][proxystore.connectors.protocols.Connector.get]).
        Connectors that use a client which is not thread-safe should guard
        it with a lock.

    Note:
        Implementations of
        [`put()`][proxystore.connectors.protocols.Connector.put] and
        [`put_batch()`][proxystore.connectors.protocols.Connector.put_batch]
        may accept additional connector-specific keyword arguments. These are
        passed via the `connector_options` parameter of
        [`Store`][proxystore.store.base.Store] methods such as
        [`Store.put()`][proxystore.store.base.Store.put].
    """

    def close(self, *, clear: bool | None = None) -> bool | None:
        """Close the connector and clean up.

        Note:
            Implementations should make this idempotent.

        Args:
            clear: Remove the objects stored by the connector (e.g., delete
                the directory used by the connector) in addition to closing
                any resources. If `None`, the default of the connector is
                used. Connectors that do not store objects which outlive the
                connector may ignore this argument.

        Returns:
            `True` if the objects stored by the connector were removed.
            The [`Store`][proxystore.store.base.Store] which owns the
            objects uses this to know that its proxies can no longer be
            resolved in this process. `None` is treated as `False`.
        """
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
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Create a new connector instance from a configuration.

        Note:
            The builtin connectors ignore unknown keys in `config`, such as
            options added by a newer version, with a
            [`VersionMismatchWarning`][proxystore.warnings.VersionMismatchWarning].

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

    def get(self, key: KeyT) -> BytesLike | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        ...

    def get_batch(self, keys: Sequence[KeyT]) -> list[BytesLike | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or \
            `None` if the corresponding key does not have an associated object.
        """
        ...

    def put(self, obj: BytesLike) -> KeyT:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.

        Returns:
            Key which can be used to retrieve the object.
        """
        ...

    def put_batch(self, objs: Sequence[BytesLike]) -> list[KeyT]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.

        Returns:
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.
        """
        ...


@runtime_checkable
class DeferrableConnector(Connector[KeyT], Protocol[KeyT]):
    """Extension of the [`Connector`][proxystore.connectors.protocols.Connector] with `set` semantics.

    Extends the [`Connector`][proxystore.connectors.protocols.Connector]
    protocol with additional methods necessary for creating a key while
    deferring associating an object with the key.
    """  # noqa: E501

    def new_key(self, obj: BytesLike | None = None) -> KeyT:
        """Create a new key.

        Note:
            Implementations may choose to require the object be provided, or
            place restrictions on the scope of the key.

        Args:
            obj: Optional object which the key will be associated with.

        Returns:
            Key which can be used to retrieve an object once \
            [`set()`][proxystore.connectors.protocols.DeferrableConnector.set] \
            has been called on the key.
        """  # noqa: E501
        ...

    def set(self, key: KeyT, obj: BytesLike) -> None:
        """Set the object associated with a key.

        Note:
            The [`Connector`][proxystore.connectors.protocols.Connector]
            provides write-once, read-many semantics. Thus,
            [`set()`][proxystore.connectors.protocols.DeferrableConnector.set]
            should only be called once per key, otherwise unexpected behavior
            can occur.

        Warning:
            This method is not required to be atomic and could therefore
            result in race conditions with calls to
            [`get()`][proxystore.connectors.protocols.Connector.get].

        Args:
            key: Key that the object will be associated with.
            obj: Object to associate with the key.
        """
        ...

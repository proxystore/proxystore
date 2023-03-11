"""Multi-connector implementation."""
from __future__ import annotations

import dataclasses
import logging
import sys
import warnings
from types import TracebackType
from typing import Any
from typing import Iterable
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import TypedDict
else:  # pragma: <3.8 cover
    from typing_extensions import TypedDict

from proxystore.connectors.connector import Connector
from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'MultiConnector is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)

logger = logging.getLogger(__name__)
KeyT = TypeVar('KeyT', bound=NamedTuple)


class PolicyDict(TypedDict):
    """JSON compatible representation of a [`Policy`][proxystore.store.multi.Policy]."""  # noqa: E501

    priority: int
    min_size: int
    max_size: int
    subset_tags: list[str]
    superset_tags: list[str]


@dataclasses.dataclass
class Policy:
    """Policy that allows validating a set of constraints."""

    priority: int = 0
    min_size: int = 0
    max_size: int = sys.maxsize
    subset_tags: list[str] = dataclasses.field(default_factory=list)
    superset_tags: list[str] = dataclasses.field(default_factory=list)

    def is_valid(
        self,
        *,
        size: int | None = None,
        subset_tags: Iterable[str] | None = None,
        superset_tags: Iterable[str] | None = None,
    ) -> bool:
        """Check if set of contstraints is valid for this policy.

        Note:
            All arguments are optional keyword arguments that default to
            `None`. If left as the default, that constraint will not be
            checked against the policy.

        Args:
            size: Object size.
            subset_tags: Set of tags that must be a subset
                of the Policy's `subset_tags` to be valid.
            superset_tags: Set of tags that must be a superset
                of the Policy's `superset_tags` to be valid.

        Returns:
            If the provided constraints are valid for the policy.
        """
        if size is not None and (size < self.min_size or size > self.max_size):
            return False
        if subset_tags is not None and not set(subset_tags).issubset(
            self.subset_tags,
        ):
            return False
        if superset_tags is not None and not set(superset_tags).issuperset(
            self.superset_tags,
        ):
            return False
        return True

    def as_dict(self) -> PolicyDict:
        """Convert the Policy to a JSON compatible dict.

        Usage:
            >>> policy = Policy(...)
            >>> policy_dict = policy.as_dict()
            >>> Policy(**policy_dict) == policy
            True
        """
        # We could use dataclasses.asdict(self) but this gives us the benefit
        # of typing on the return dict.
        return PolicyDict(
            priority=self.priority,
            min_size=self.min_size,
            max_size=self.max_size,
            subset_tags=self.subset_tags,
            superset_tags=self.superset_tags,
        )


class _ConnectorPolicy(NamedTuple):
    connector: Connector[Any]
    policy: Policy


class MultiKey(NamedTuple):
    """Key to objects in [`MultiConnector`][proxystore.connectors.multi.MultiConnector]."""  # noqa: E501

    connector_name: str
    """Name of connector that the associated object is stored in."""
    # Type this as Any because mypy has no way to tell statically what type
    # of key this is. In reality it is a NamedTuple. Otherwise you end up with
    # something like: Argument 1 to "exists" of "LocalConnector" has
    # incompatible type "NamedTuple"; expected "LocalKey"  [arg-type]
    connector_key: Any
    """Key associated with the object."""


class MultiConnector:
    """Policy based manager for a [`Connector`][proxystore.connectors.connector.Connector] collection.

    Args:
        connectors: Mapping of names to tuples of a
            [`Connector`][proxystore.connectors.connector.Connector] and
            [`Policy`][proxystore.store.multi.Policy].
    """  # noqa: E501

    def __init__(
        self,
        connectors: dict[str, tuple[Connector[Any], Policy]],
    ) -> None:
        self.connectors = {
            name: _ConnectorPolicy(connector, policy)
            for name, (connector, policy) in connectors.items()
        }

        names = list(self.connectors.keys())
        self.connectors_by_priority = sorted(
            names,
            key=lambda name: self.connectors[name].policy.priority,
            reverse=True,
        )

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
        return f'{self.__class__.__name__}({self.connectors})'

    def close(self) -> None:
        """Close the connector and clean up.

        Warning:
            This will call `close()` on all managed connectors.
        """
        for connector, _ in self.connectors.values():
            connector.close()

    def config(self) -> dict[str, Any]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        return {
            name: (connector, policy.as_dict())
            for name, (connector, policy) in self.connectors.items()
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MultiConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        connectors = {
            name: (connector, Policy(**policy))
            for name, (connector, policy) in config.items()
        }
        return cls(connectors=connectors)

    def evict(self, key: MultiKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        connector = self.connectors[key.connector_name].connector
        connector.evict(key.connector_key)

    def exists(self, key: MultiKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        connector = self.connectors[key.connector_name].connector
        return connector.exists(key.connector_key)

    def get(self, key: MultiKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        connector = self.connectors[key.connector_name].connector
        return connector.get(key.connector_key)

    def get_batch(self, keys: Sequence[MultiKey]) -> list[bytes | None]:
        """Get a batch of serialized objects associated with the keys.

        Args:
            keys: Sequence of keys associated with objects to retrieve.

        Returns:
            List with same order as `keys` with the serialized objects or
            `None` if the corresponding key does not have an associated object.
        """
        return [self.get(key) for key in keys]

    def put(
        self,
        obj: bytes,
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
    ) -> MultiKey:
        """Put a serialized object in the store.

        Args:
            obj: Serialized object to put in the store.
            subset_tags: Iterable of tags that must be a subset
                of a connector's policy `subset_tags` to match.
            superset_tags: Iterable of tags that must be a superset
                of a connectors's policy `superset_tags` to match.

        Returns:
            Key which can be used to retrieve the object.

        Raises:
            RuntimeError: If no connector policy matches the arguments.
        """
        for connector_name in self.connectors_by_priority:
            connector, policy = self.connectors[connector_name]
            if policy.is_valid(
                size=len(obj),
                subset_tags=subset_tags,
                superset_tags=superset_tags,
            ):
                key = connector.put(obj)
                return MultiKey(
                    connector_name=connector_name,
                    connector_key=key,
                )
        else:
            raise RuntimeError(
                'No connector policy was suitable for the constraints: '
                f'subset_tags={subset_tags}, superset_tags={superset_tags}.',
            )

    def put_batch(
        self,
        objs: Sequence[bytes],
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
    ) -> list[MultiKey]:
        """Put a batch of serialized objects in the store.

        Args:
            objs: Sequence of serialized objects to put in the store.
            subset_tags: Iterable of tags that must be a subset
                of a connector's policy `subset_tags` to match.
            superset_tags: Iterable of tags that must be a superset
                of a connectors's policy `superset_tags` to match.

        Returns:
            List of keys with the same order as `objs` which can be used to
            retrieve the objects.


        Raises:
            RuntimeError: If no connector policy matches the arguments.
        """
        return [
            self.put(obj, subset_tags=subset_tags, superset_tags=superset_tags)
            for obj in objs
        ]

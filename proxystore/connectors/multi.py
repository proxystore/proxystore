"""Multi-connector implementation."""
from __future__ import annotations

import dataclasses
import logging
import re
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

from proxystore import utils
from proxystore.connectors.connector import Connector
from proxystore.utils import get_class_path
from proxystore.utils import import_class
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
    host_pattern: list[str] | str | None
    min_size_bytes: int
    max_size_bytes: int
    subset_tags: list[str]
    superset_tags: list[str]


@dataclasses.dataclass
class Policy:
    """Policy that allows validating a set of constraints."""

    priority: int = 0
    """Priority for breaking ties between policies (higher is preferred)."""
    host_pattern: Iterable[str] | str | None = None
    """Pattern or iterable of patterns of valid hostnames.

    The hostname returned by [`hostname()`][proxystore.utils.hostname] is
    matched against `host_pattern` using [`re.fullmatch()`][re.fullmatch]. If
    `host_pattern` is an iterable, at least one of the patterns must match
    the hostname.
    """
    min_size_bytes: int = 0
    """Minimum size in bytes allowed."""
    max_size_bytes: int = sys.maxsize
    """Maximum size in bytes allowed."""
    subset_tags: list[str] = dataclasses.field(default_factory=list)
    """Subset tags."""
    superset_tags: list[str] = dataclasses.field(default_factory=list)
    """Superset tags."""

    def is_valid(
        self,
        *,
        size_bytes: int | None = None,
        subset_tags: Iterable[str] | None = None,
        superset_tags: Iterable[str] | None = None,
    ) -> bool:
        """Check if set of contstraints is valid for this policy.

        Note:
            All arguments are optional keyword arguments that default to
            `None`. If left as the default, that constraint will not be
            checked against the policy.

        Args:
            size_bytes: Object size in bytes.
            subset_tags: Set of tags that must be a subset
                of the Policy's `subset_tags` to be valid.
            superset_tags: Set of tags that must be a superset
                of the Policy's `superset_tags` to be valid.

        Returns:
            If the provided constraints are valid for the policy.
        """
        if size_bytes is not None and (
            size_bytes < self.min_size_bytes
            or size_bytes > self.max_size_bytes
        ):
            return False
        if subset_tags is not None and not set(subset_tags).issubset(
            self.subset_tags,
        ):
            return False
        if superset_tags is not None and not set(superset_tags).issuperset(
            self.superset_tags,
        ):
            return False
        return self.is_valid_on_host()

    def is_valid_on_host(self) -> bool:
        """Check if this policy is valid on the current host."""
        if self.host_pattern is None:
            return True

        patterns: Iterable[str]
        if isinstance(self.host_pattern, str):
            patterns = [self.host_pattern]
        else:
            patterns = self.host_pattern
        hostname = utils.hostname()
        return any(re.fullmatch(p, hostname) for p in patterns)

    def as_dict(self) -> PolicyDict:
        """Convert the Policy to a JSON compatible dict.

        Example:
            ```python
            >>> policy = Policy(...)
            >>> policy_dict = policy.as_dict()
            >>> Policy(**policy_dict) == policy
            True
            ```
        """
        # We could use dataclasses.asdict(self) but this gives us the benefit
        # of typing on the return dict.
        return PolicyDict(
            priority=self.priority,
            host_pattern=(
                list(self.host_pattern)
                if isinstance(self.host_pattern, Iterable)
                else self.host_pattern
            ),
            min_size_bytes=self.min_size_bytes,
            max_size_bytes=self.max_size_bytes,
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

    Example:
        ```python
        from proxystore.connectors.file import FileConnector
        from proxystore.connectors.multi import Policy
        from proxystore.connectors.multi import MultiConnector
        from proxystore.connectors.redis import RedisConnector

        file_connector = FileConnector(...)
        redis_connector = RedisConnector(...)

        connectors = {
            'small': (file_connector, Policy(max_size_bytes=1000000)),
            'large': (redis_connector, Policy(min_size_bytes=1000000)),
        }
        connector = MultiConnector(connector)
        ```

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
            name: (
                get_class_path(type(connector)),
                connector.config(),
                policy.as_dict(),
            )
            for name, (connector, policy) in self.connectors.items()
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MultiConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        connectors: dict[str, tuple[Connector[Any], Policy]] = {}
        for name, (connector_path, connector_config, policy) in config.items():
            connector_type = import_class(connector_path)
            connector = connector_type.from_config(connector_config)
            policy = Policy(**policy)
            connectors[name] = (connector, policy)
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
            List with same order as `keys` with the serialized objects or \
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
                size_bytes=len(obj),
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
            List of keys with the same order as `objs` which can be used to \
            retrieve the objects.

        Raises:
            RuntimeError: If no connector policy matches the arguments.
        """
        return [
            self.put(obj, subset_tags=subset_tags, superset_tags=superset_tags)
            for obj in objs
        ]

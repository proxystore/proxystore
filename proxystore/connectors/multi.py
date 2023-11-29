"""Multi-connector implementation."""
from __future__ import annotations

import dataclasses
import logging
import re
import sys
import warnings
from types import TracebackType
from typing import Any
from typing import Dict
from typing import Iterable
from typing import NamedTuple
from typing import Sequence
from typing import Tuple
from typing import TypedDict
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore import utils
from proxystore.connectors.protocols import Connector
from proxystore.utils.imports import get_class_path
from proxystore.utils.imports import import_class
from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'MultiConnector is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)

logger = logging.getLogger(__name__)
KeyT = TypeVar('KeyT', bound=NamedTuple)


class PolicyDict(TypedDict):
    """JSON compatible representation of a [`Policy`][proxystore.connectors.multi.Policy]."""  # noqa: E501

    priority: int
    host_pattern: list[str] | str | None
    min_size_bytes: int
    max_size_bytes: int
    subset_tags: list[str]
    superset_tags: list[str]


@dataclasses.dataclass
class Policy:
    """Policy that allows validating a set of constraints.

    Attributes:
        priority: Priority for breaking ties between policies (higher is
            preferred).
        host_pattern: Pattern or iterable of patterns of valid hostnames.
            The hostname returned by [`hostname()`][proxystore.utils.hostname]
            is matched against `host_pattern` using
            [`re.fullmatch()`][re.fullmatch]. If `host_pattern` is an iterable,
            at least one of the patterns must match the hostname.
        min_size_bytes: Minimum size in bytes allowed.
        max_size_bytes: Maximum size in bytes allowed.
        subset_tags: Subset tags. See
            [`is_valid()`][proxystore.connectors.multi.Policy.is_valid] for
            more details.
        superset_tags: Superset tags. See
            [`is_valid()`][proxystore.connectors.multi.Policy.is_valid] for
            more details.
    """

    priority: int = 0
    host_pattern: Iterable[str] | str | None = None
    min_size_bytes: int = 0
    max_size_bytes: int = sys.maxsize
    subset_tags: list[str] = dataclasses.field(default_factory=list)
    superset_tags: list[str] = dataclasses.field(default_factory=list)

    def is_valid(
        self,
        *,
        size_bytes: int | None = None,
        subset_tags: Iterable[str] | None = None,
        superset_tags: Iterable[str] | None = None,
    ) -> bool:
        """Check if set of constraints is valid for this policy.

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
        host_pattern = (
            self.host_pattern
            if isinstance(self.host_pattern, str) or self.host_pattern is None
            else list(self.host_pattern)
        )
        return PolicyDict(
            priority=self.priority,
            host_pattern=host_pattern,
            min_size_bytes=self.min_size_bytes,
            max_size_bytes=self.max_size_bytes,
            subset_tags=self.subset_tags,
            superset_tags=self.superset_tags,
        )


class _ConnectorPolicy(NamedTuple):
    connector: Connector[Any]
    policy: Policy


ConnectorPolicyConfig = Tuple[str, Dict[str, Any], PolicyDict]
"""Type of the configuration for a connector and policy pair.

Element zero is the fully qualified path of the connector type,
element one is the connector's configuration dictionary, and
element two is the policy in dictionary form.
"""


class MultiConnectorError(Exception):
    """Exceptions raised by the [`MultiConnector`][proxystore.connectors.multi.MultiConnector]."""  # noqa: E501


class MultiKey(NamedTuple):
    """Key to objects in [`MultiConnector`][proxystore.connectors.multi.MultiConnector].

    Attributes:
        connector_name: Name of connector that the associated object is
            stored in.
        connector_key: Key associated with the object.
    """  # noqa: E501

    connector_name: str
    # Type this as Any because mypy has no way to tell statically what type
    # of key this is. In reality it is a NamedTuple. Otherwise you end up with
    # something like: Argument 1 to "exists" of "LocalConnector" has
    # incompatible type "NamedTuple"; expected "LocalKey"  [arg-type]
    connector_key: Any


class MultiConnector:
    """Policy based manager for a [`Connector`][proxystore.connectors.protocols.Connector] collection.

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

    Note:
        Methods of this class will raise
        [`MultiConnectorError`][proxystore.connectors.multi.MultiConnectorError]
        if they are passed an invalid key where a key could be invalid
        because the connector which created the key is not known by this class
        instance or because the corresponding connector is dormant.

    Args:
        connectors: Mapping of names to tuples of a
            [`Connector`][proxystore.connectors.protocols.Connector] and
            [`Policy`][proxystore.connectors.multi.Policy].
        dormant_connectors: Mapping of names to tuples containing the
            configuration of a dormant connector. A dormant connector is
            a connector that is unused in this process, but could potentially
            be initialized and used on another process. For example,
            because the `host_pattern` of the policy does not match the
            current host. It is not recommended to create dormant connector
            configurations yourself. Rather, create your connectors and
            use the `host_pattern` of the policy to determine when a connector
            should be dormant.
    """  # noqa: E501

    def __init__(
        self,
        connectors: dict[str, tuple[Connector[Any], Policy]],
        dormant_connectors: dict[str, ConnectorPolicyConfig] | None = None,
    ) -> None:
        self.connectors = {
            name: _ConnectorPolicy(connector, policy)
            for name, (connector, policy) in connectors.items()
        }
        self.dormant_connectors = dormant_connectors

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

    def _connector_from_key(self, key: MultiKey) -> Connector[Any]:
        if key.connector_name in self.connectors:
            return self.connectors[key.connector_name].connector
        elif (
            self.dormant_connectors is not None
            and key.connector_name in self.dormant_connectors
        ):
            raise MultiConnectorError(
                f'The connector associated with {key} is dormant.',
            )
        else:
            raise MultiConnectorError(
                f'The connector which created {key} does not exist.',
            )

    def close(self) -> None:
        """Close the connector and clean up.

        Warning:
            This will call `close()` on all managed connectors.
        """
        for connector, _ in self.connectors.values():
            connector.close()

    def config(self) -> dict[str, ConnectorPolicyConfig]:
        """Get the connector configuration.

        The configuration contains all the information needed to reconstruct
        the connector object.
        """
        configs: dict[str, ConnectorPolicyConfig] = (
            self.dormant_connectors
            if self.dormant_connectors is not None
            else {}
        )
        configs.update(
            {
                name: (
                    get_class_path(type(connector)),
                    connector.config(),
                    policy.as_dict(),
                )
                for name, (connector, policy) in self.connectors.items()
            },
        )
        return configs

    @classmethod
    def from_config(
        cls,
        config: dict[str, ConnectorPolicyConfig],
    ) -> MultiConnector:
        """Create a new connector instance from a configuration.

        Args:
            config: Configuration returned by `#!python .config()`.
        """
        connectors: dict[str, tuple[Connector[Any], Policy]] = {}
        dormant_connectors: dict[str, ConnectorPolicyConfig] = {}
        for name, (conn_path, conn_config, policy_dict) in config.items():
            policy = Policy(**policy_dict)
            if policy.is_valid_on_host():
                connector_type = import_class(conn_path)
                connector = connector_type.from_config(conn_config)
                connectors[name] = (connector, policy)
            else:
                dormant_connectors[name] = config[name]
        return cls(
            connectors=connectors,
            dormant_connectors=dormant_connectors,
        )

    def evict(self, key: MultiKey) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        connector = self._connector_from_key(key)
        connector.evict(key.connector_key)

    def exists(self, key: MultiKey) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        connector = self._connector_from_key(key)
        return connector.exists(key.connector_key)

    def get(self, key: MultiKey) -> bytes | None:
        """Get the serialized object associated with the key.

        Args:
            key: Key associated with the object to retrieve.

        Returns:
            Serialized object or `None` if the object does not exist.
        """
        connector = self._connector_from_key(key)
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
            MultiConnectorError: If no connector policy matches the arguments.
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
        raise MultiConnectorError(
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

        Warning:
            This method calls
            [`put()`][proxystore.connectors.multi.MultiConnector] individually
            for each item in the batch so items in the batch can potentially
            be placed in different connectors.

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
            MultiConnectorError: If no connector policy matches the arguments.
        """
        return [
            self.put(obj, subset_tags=subset_tags, superset_tags=superset_tags)
            for obj in objs
        ]

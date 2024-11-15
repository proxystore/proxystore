"""Store configuration model."""

from __future__ import annotations

import pathlib
import sys
from typing import Any
from typing import Dict  # noqa: UP035
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from proxystore.connectors.protocols import Connector
from proxystore.store.types import DeserializerT
from proxystore.store.types import SerializerT
from proxystore.utils.config import dump
from proxystore.utils.config import load
from proxystore.utils.imports import import_from_path

_KNOWN_CONNECTORS = {
    'proxystore.connectors.endpoint.EndpointConnector',
    'proxystore.connectors.file.FileConnector',
    'proxystore.connectors.globus.GlobusConnector',
    'proxystore.connectors.local.LocalConnector',
    'proxystore.connectors.multi.MultiConnector',
    'proxystore.connectors.redis.RedisConnector',
}


class ConnectorConfig(BaseModel):
    """Connector configuration.

    Example:
        ```python
        from proxystore.connectors.redis import RedisConnector
        from proxystore.store.config import ConnectorConfig

        config = ConnectorConfig(
            kind='redis',
            options={'hostname': 'localhost', 'port': 1234},
        )

        connector = config.get_connector()
        assert isinstance(connector, RedisConnector)
        ```

    Attributes:
        kind: Fully-qualified path used to import a
            [`Connector`][proxystore.connectors.protocols.Connector]
            type or a shortened name for a builtin
            [`Connector`][proxystore.connectors.protocols.Connector] type.
            E.g., `'file'` or `'FileConnector'` are valid shortcuts for
            `'proxystore.connectors.file.FileConnector'`.
        options: Dictionary of keyword arguments to pass to the
            [`Connector`][proxystore.connectors.protocols.Connector]
            constructor.
    """

    model_config = ConfigDict(extra='forbid')

    kind: str
    options: Dict[str, Any] = Field(default_factory=dict)  # noqa: UP006

    def get_connector_type(self) -> type[Connector[Any]]:
        """Resolve the class type for the specified connector.

        This method works by first attempting to import the class
        by treating `kind` as a fully-qualified path. For example,
        if `kind='proxystore.connectors.local.LocalConnector'`, the
        [`LocalConnector`][proxystore.connectors.local.LocalConnector] is
        imported from
        [`proxystore.connectors.local`][proxystore.connectors.local].

        If the import fails, `kind` will be checked against a list of known
        (i.e., builtin)
        [`Connector`][proxystore.connectors.protocols.Connector] types.
        `kind` will be psuedo-fuzzy matched against the class names of the
        known [`Connector`][proxystore.connectors.protocols.Connector] types.
        For example, `kind='local'` and `kind='LocalConnector'` will both
        match to `'proxystore.connectors.local.LocalConnector'`.

        Returns:
            [`Connector`][proxystore.connectors.protocols.Connector] type.

        Raises:
            ValueError: If a
                [`Connector`][proxystore.connectors.protocols.Connector]
                named `kind` failed to import or if `kind` does not match
                a builtin connector.
        """
        try:
            return import_from_path(self.kind)
        except ImportError as e:
            for path in _KNOWN_CONNECTORS:
                _, name = path.rsplit('.', 1)
                name = name.lower()
                choices = [name, name.replace('connector', '')]
                if self.kind.lower() in choices:
                    return import_from_path(path)
            raise ValueError(f'Unknown connector type "{self.kind}".') from e

    def get_connector(self) -> Connector[Any]:
        """Get the connector specified by the configuration.

        Returns:
            A [`Connector`][proxystore.connectors.protocols.Connector] \
            instance.
        """
        connector_type = self.get_connector_type()
        return connector_type(**self.options)


class StoreConfig(BaseModel):
    """Store configuration.

    Tip:
        See the [`Store`][proxystore.store.base.Store] parameters for more
        information about each configuration option.

    Attributes:
        name: Store name.
        connector: Connector configuration.
        serializer: Optional serializer.
        deserializer: Optional deserializer.
        cache_size: Cache size.
        metrics: Enable recording operation metrics.
        populate_target: Set the default value for the `populate_target`
            parameter of proxy methods.
        auto_register: Auto-register the store.
    """

    model_config = ConfigDict(extra='forbid')

    name: str
    connector: ConnectorConfig
    serializer: Optional[SerializerT] = Field(None)  # noqa: UP007
    deserializer: Optional[DeserializerT] = Field(None)  # noqa: UP007
    cache_size: int = Field(16)
    metrics: bool = Field(False)
    populate_target: bool = Field(True)
    auto_register: bool = Field(False)

    @classmethod
    def from_toml(cls, filepath: str | pathlib.Path) -> Self:
        """Create a configuration file from a TOML file.

        Example:
            See
            [`write_toml()`][proxystore.store.config.StoreConfig.write_toml].

        Args:
            filepath: Path to TOML file to load.
        """
        with open(filepath, 'rb') as f:
            return load(cls, f)

    def write_toml(self, filepath: str | pathlib.Path) -> None:
        """Write a configuration to a TOML file.

        Example:
            ```python
            from proxystore.store.config import ConnectorConfig
            from proxystore.store.config import StoreConfig

            config = StoreConfig(
                name='example',
                connector=ConnectorConfig(
                    kind='file',
                    options={'store_dir': '/tmp/proxystore-cache'},
                ),
            )

            config.write_toml('config.toml')
            ```
            The resulting TOML file contains the full configuration,
            including default options, and can be loaded again
            using `#!python StoreConfig.from_toml('config.toml')`.
            ```toml title="config.toml"
            name = "example"
            cache_size = 16
            metrics = false
            populate_target = true
            auto_register = false

            [connector]
            kind = "file"

            [connector.options]
            store_dir = "/tmp/proxystore-cache"
            ```

        Args:
            filepath: Path to TOML file to write.
        """
        filepath = pathlib.Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            dump(self, f)

"""Endpoint configuration."""

from __future__ import annotations

import re
import socket
from typing import Any
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from proxystore.endpoint.files import check_format_version
from proxystore.endpoint.identity import EndpointId
from proxystore.utils.environment import hostname

MAX_OBJECT_SIZE_DEFAULT = 100_000_000
"""Default maximum endpoint object size in bytes."""
DEFAULT_DATABASE_PATH = 'blobs.db'
"""Default path of the SQLite database, relative to the endpoint
directory."""
CONFIG_VERSION = 1
"""Format version of the endpoint configuration file."""


class EndpointStorageConfig(BaseModel):
    """Endpoint data storage configuration.

    Attributes:
        backend: Storage backend of the endpoint. `"memory"` stores objects
            in memory so objects are lost when the endpoint stops.
            `"sqlite"` stores objects in a SQLite database.
        database_path: Path of the SQLite database. Only valid with the
            `"sqlite"` backend, and defaults to
            [`DEFAULT_DATABASE_PATH`][proxystore.endpoint.config.DEFAULT_DATABASE_PATH].
            A relative path is relative to the endpoint directory and an
            absolute path can be used to store the database elsewhere
            (e.g., on a larger file system). `~` is expanded to the user's
            home directory.

    Raises:
        ValueError: If `database_path` is set with the `"memory"` backend.
    """

    model_config = ConfigDict(extra='forbid')

    backend: Literal['memory', 'sqlite'] = 'memory'
    database_path: str | None = None

    @model_validator(mode='after')
    def _database_path_validator(self) -> Self:
        if self.backend != 'sqlite' and self.database_path is not None:
            raise ValueError(
                'The database_path is only used by the "sqlite" backend.',
            )
        return self


class EndpointP2PConfig(BaseModel):
    """Endpoint peer-to-peer configuration.

    Attributes:
        enabled: Enable communication with peer endpoints. Only endpoints
            in the allowlist of peers (`peers.toml` in the endpoint
            directory) can communicate with this endpoint.
        relays: Relay servers used to establish connections with peers
            and to relay traffic when a direct connection is not possible.
            `"n0"` uses the public relays operated by n0 (the developers of
            iroh), `"none"` disables relays, and a list of URLs uses
            self-hosted `iroh-relay` servers.
    """

    model_config = ConfigDict(extra='forbid')

    enabled: bool = True
    relays: Literal['n0', 'none'] | list[str] = 'n0'

    @field_validator('relays')
    @classmethod
    def _relays_validator(
        cls,
        v: Literal['n0', 'none'] | list[str],
    ) -> Literal['n0', 'none'] | list[str]:
        if isinstance(v, list):
            if len(v) == 0:
                raise ValueError(
                    'Relays must contain at least one URL. Use "none" to '
                    'disable relays.',
                )
            for url in v:
                if not url.startswith(('http://', 'https://')):
                    raise ValueError(
                        f'Relay URL must start with http:// or https://. '
                        f'Got {url}.',
                    )
        return v


class EndpointConfig(BaseModel):
    """Endpoint configuration.

    Attributes:
        version: Format version of the configuration.
        name: Endpoint name. Must match the name of the endpoint directory.
        id: Endpoint ID. This is the public key of the endpoint's secret key
            which is stored separately in the endpoint directory.
        host: Address clients use to connect to the endpoint. `"ip"` or
            `"fqdn"` use the IP address or fully-qualified domain name of the
            host, determined each time the endpoint starts. Any other value
            is used as a static address (e.g., `"127.0.0.1"`).
        port: Port endpoint is running on.
        tls: Encrypt connections between clients and the endpoint with TLS.
            The endpoint generates a self-signed certificate each time it
            starts, and clients only trust that certificate.
        max_object_size: Maximum size in bytes of an object that clients
            or peers can set on the endpoint. If `0`, there is no limit.
        p2p: Peer-to-peer configuration.
        storage: Storage configuration.

    Raises:
        ValueError: If the name does not contain only alphanumeric, dash, or
            underscore characters, if the ID cannot be parsed, if the
            port is not in the range [1, 65535], if the host is empty, if
            the version is not supported, if the maximum object size is
            negative, or if there are unknown fields.
    """

    model_config = ConfigDict(extra='forbid')

    version: int = CONFIG_VERSION
    name: str
    id: EndpointId
    port: int
    host: str = 'ip'
    tls: bool = False
    max_object_size: int = MAX_OBJECT_SIZE_DEFAULT
    p2p: EndpointP2PConfig = Field(default_factory=EndpointP2PConfig)
    storage: EndpointStorageConfig = Field(
        default_factory=EndpointStorageConfig,
    )

    @field_validator('name')
    @classmethod
    def _name_validator(cls, v: str) -> str:
        if not validate_name(v):
            raise ValueError(
                'Name must only contain alphanumeric characters, dashes, and '
                f' underscores. Got {v}.',
            )
        return v

    @model_validator(mode='before')
    @classmethod
    def _legacy_uuid_validator(cls, data: Any) -> Any:
        if isinstance(data, dict) and 'uuid' in data and 'id' not in data:
            raise ValueError(
                'The configuration was created by an older version of '
                'ProxyStore which identified endpoints by UUID. Remove the '
                'endpoint and configure it again with '
                '"proxystore-endpoint configure".',
            )
        return data

    @field_validator('version')
    @classmethod
    def _version_validator(cls, v: int) -> int:
        return check_format_version(v, CONFIG_VERSION, 'configuration')

    @field_validator('host')
    @classmethod
    def _host_validator(cls, v: str) -> str:
        if len(v.strip()) == 0:
            raise ValueError(
                'Host must be "ip", "fqdn", or an address. Got an empty '
                'string.',
            )
        return v.strip()

    @field_validator('port')
    @classmethod
    def _port_validator(cls, v: int) -> int:
        if v < 1 or v > 65535:
            raise ValueError('Port must be in range [1, 65535].')
        return v

    @field_validator('max_object_size')
    @classmethod
    def _max_object_size_validator(cls, v: int) -> int:
        if v < 0:
            raise ValueError(
                'Max object size must be zero (no limit) or greater.',
            )
        return v

    @property
    def object_size_limit(self) -> int | None:
        """Maximum object size in bytes or `None` if there is no limit."""
        return self.max_object_size if self.max_object_size > 0 else None


def resolve_host(host: str) -> str:
    """Resolve the address clients use to connect to the endpoint.

    Args:
        host: `"ip"`, `"fqdn"`, or a static address (see
            [`EndpointConfig.host`][proxystore.endpoint.config.EndpointConfig]).

    Returns:
        The IP address or fully-qualified domain name of this host, or the \
        static address.
    """
    if host == 'fqdn':
        return socket.getfqdn()
    if host == 'ip':
        return socket.gethostbyname(hostname())
    return host


def validate_name(name: str) -> bool:
    """Validate name only contains alphanumeric or dash/underscore chars."""
    return len(re.findall(r'[^A-Za-z0-9_\-]', name)) == 0 and len(name) > 0

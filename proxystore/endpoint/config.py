"""Endpoint configuration."""

from __future__ import annotations

import re
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from proxystore.endpoint.identity import EndpointId

MAX_OBJECT_SIZE_DEFAULT = 100_000_000
"""Default maximum endpoint object size in bytes."""


class EndpointStorageConfig(BaseModel):
    """Endpoint data storage configuration.

    Attributes:
        database_path: Optional path to SQLite database file that will be used
            for storing endpoint data. If `None`, data will only be stored
            in-memory.
        max_object_size: Maximum object size in bytes. If `0`, there is no
            limit on object sizes.
    """

    database_path: str | None = None
    max_object_size: int = MAX_OBJECT_SIZE_DEFAULT

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
        name: Endpoint name.
        id: Endpoint ID. This is the public key of the endpoint's secret key
            which is stored separately in the endpoint directory.
        host: Host endpoint is running on.
        host_type: Type of host address to use. If `"ip"` or `"fqdn"`, the
            host is determined when the endpoint starts. If `"static"`, the
            `host` field is used.
        port: Port endpoint is running on.
        tls: Encrypt connections between clients and the endpoint with TLS.
            The endpoint generates a self-signed certificate each time it
            starts, and clients only trust that certificate.
        p2p: Peer-to-peer configuration.
        storage: Storage configuration.

    Raises:
        ValueError: If the name does not contain only alphanumeric, dash, or
            underscore characters, if the ID cannot be parsed, or if the
            port is not in the range [1, 65535].
    """

    name: str
    id: EndpointId
    port: int
    host: str | None = None
    host_type: Literal['fqdn', 'ip', 'static'] = 'ip'
    tls: bool = False
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

    @field_validator('port')
    @classmethod
    def _port_validator(cls, v: int) -> int:
        if v < 1 or v > 65535:
            raise ValueError('Port must be in range [1, 65535].')
        return v


def validate_name(name: str) -> bool:
    """Validate name only contains alphanumeric or dash/underscore chars."""
    return len(re.findall(r'[^A-Za-z0-9_\-]', name)) == 0 and len(name) > 0

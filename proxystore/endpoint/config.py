"""Endpoint configuration."""

from __future__ import annotations

import re
import uuid
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

try:
    from pydantic import field_validator
except ImportError:  # pragma: no cover
    # Pydantic v1 compatibility
    from pydantic import validator as field_validator  # type: ignore[no-redef]

MAX_OBJECT_SIZE_DEFAULT = 100_000_000
"""Default maximum endpoint object size in bytes."""


class EndpointRelayAuthConfig(BaseModel):
    """Endpoint relay server authentication configuration.

    Attributes:
        method: Relay server authentication method.
        kwargs: Arbitrary options used by the authentication method.
    """

    model_config = ConfigDict(extra='forbid')

    method: Literal['globus'] | None = None
    kwargs: dict[str, Any] = Field(default_factory=dict)


class EndpointRelayICEServerConfig(BaseModel):
    """STUN/TURN server used when gathering ICE candidates.

    Attributes:
        urls: One or more URLs of the STUN or TURN server (e.g.,
            `stun:stun.l.google.com:19302`).
        username: Optional username for authenticating with a TURN server.
        credential: Optional credential for authenticating with a TURN server.
    """

    model_config = ConfigDict(extra='forbid')

    urls: str | list[str]
    username: str | None = None
    credential: str | None = None


class EndpointRelayConfig(BaseModel):
    """Endpoint relay server configuration.

    Attributes:
        address: Address of the relay server to register with.
        auth: Relay server authentication configuration.
        ice_servers: STUN/TURN servers to use when gathering ICE candidates
            for peer connections. If `None`, a default set of public STUN
            servers is used. An empty list disables server-reflexive candidate
            gathering entirely, which is useful when all peers are on the same
            host or when STUN servers are unreachable.
        peer_channels: Number of peer channels to multiplex communication over.
        verify_certificates: Validate the relay server's SSL certificate. This
            should only be disabled when testing endpoint with local relay
            servers using self-signed certificates.
    """

    address: str | None = None
    auth: EndpointRelayAuthConfig = Field(
        default_factory=EndpointRelayAuthConfig,
    )
    ice_servers: list[EndpointRelayICEServerConfig] | None = None
    peer_channels: int = 1
    verify_certificate: bool = True

    @field_validator('address')
    @classmethod
    def _address_validator(cls, v: str | None) -> str | None:
        if v is not None and not (
            v.startswith('ws://') or v.startswith('wss://')
        ):
            raise ValueError(
                'Server must start with ws:// or wss://.',
            )
        return v

    @field_validator('peer_channels')
    @classmethod
    def _peer_channels_validator(cls, v: int) -> int:
        if v < 1:
            raise ValueError('Peer channels must be >= 1.')
        return v


class EndpointStorageConfig(BaseModel):
    """Endpoint data storage configuration.

    Attributes:
        database_path: Optional path to SQLite database file that will be used
            for storing endpoint data. If `None`, data will only be stored
            in-memory.
        max_object_size: Optional maximum object size.
    """

    database_path: str | None = None
    max_object_size: int = MAX_OBJECT_SIZE_DEFAULT

    @field_validator('max_object_size')
    @classmethod
    def _max_object_size_validator(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError(
                'Max object size must be None or greater than zero.',
            )
        return v


class EndpointConfig(BaseModel):
    """Endpoint configuration.

    Attributes:
        name: Endpoint name.
        uuid: Endpoint UUID.
        host: Host endpoint is running on.
        host_type: Type of host address to use (FQDN or IP).
        port: Port endpoint is running on.
        tls: Encrypt connections between clients and the endpoint with TLS.
            The endpoint generates a self-signed certificate each time it
            starts, and clients only trust that certificate.
        peering: Peering configuration.
        storage: Storage configuration.

    Raises:
        ValueError: If the name does not contain only alphanumeric, dash, or
            underscore characters, if the UUID cannot be parsed, or if the
            port is not in the range [1, 65535].
    """

    name: str
    uuid: str
    port: int
    host: str | None = None
    host_type: Literal['fqdn', 'ip', 'static'] = 'ip'
    tls: bool = False
    relay: EndpointRelayConfig = Field(
        default_factory=EndpointRelayConfig,
    )
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

    @field_validator('uuid')
    @classmethod
    def _uuid_validator(cls, v: str) -> str:
        try:
            uuid.UUID(v, version=4)
        except ValueError:
            raise ValueError(
                f'"{v}" is not a valid UUID4 string.',
            ) from None
        return v

    @field_validator('port')
    @classmethod
    def _port_validator(cls, v: int) -> int:
        if v < 1 or v > 65535:
            raise ValueError('Port must be in range [1, 65535].')
        return v


def validate_name(name: str) -> bool:
    """Validate name only contains alphanumeric or dash/underscore chars."""
    return len(re.findall(r'[^A-Za-z0-9_\-]', name)) == 0 and len(name) > 0

"""Endpoint configuration."""

from __future__ import annotations

import re
import uuid
from typing import Literal

from pydantic import BaseModel
from pydantic import Field

try:
    from pydantic import field_validator
except ImportError:  # pragma: no cover
    # Pydantic v1 compatibility
    from pydantic import validator as field_validator  # type: ignore[no-redef]

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


class EndpointConfig(BaseModel):
    """Endpoint configuration.

    Attributes:
        name: Endpoint name.
        uuid: Endpoint UUID.
        host: Host endpoint is running on.
        host_type: Type of host address to use. If `"ip"` or `"fqdn"`, the
            host is determined when the endpoint starts. If `"static"`, the
            `host` field is used.
        port: Port endpoint is running on.
        tls: Encrypt connections between clients and the endpoint with TLS.
            The endpoint generates a self-signed certificate each time it
            starts, and clients only trust that certificate.
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

"""Endpoint configuration."""
from __future__ import annotations

import os
import re
import uuid
from typing import Any
from typing import Dict
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

from proxystore.endpoint.constants import MAX_OBJECT_SIZE_DEFAULT
from proxystore.utils.config import dump
from proxystore.utils.config import load

ENDPOINT_CONFIG_FILE = 'config.toml'
ENDPOINT_DATABASE_FILE = 'blobs.db'
ENDPOINT_LOG_FILE = 'log.txt'
ENDPOINT_PID_FILE = 'daemon.pid'


class EndpointRelayAuthConfig(BaseModel):
    """Endpoint relay server authentication configuration.

    Attributes:
        method: Relay server authentication method.
        kwargs: Arbitrary options used by the authentication method.
    """

    method: Optional[Literal['globus']] = None  # noqa: UP007
    kwargs: Dict[str, Any] = Field(default_factory=dict)  # noqa: UP006


class EndpointRelayConfig(BaseModel):
    """Endpoint relay server configuration.

    Attributes:
        address: Address of the relay server to register with.
        auth: Relay server authentication configuration.
        peer_channels: Number of peer channels to multiplex communication over.
        verify_certificates: Validate the relay server's SSL certificate. This
            should only be disabled when testing endpoint with local relay
            servers using self-signed certificates.
    """

    address: Optional[str] = None  # noqa: UP007
    auth: EndpointRelayAuthConfig = Field(
        default_factory=EndpointRelayAuthConfig,
    )
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

    database_path: Optional[str] = None  # noqa: UP007
    max_object_size: Optional[int] = MAX_OBJECT_SIZE_DEFAULT  # noqa: UP007

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
        port: Port endpoint is running on.
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
    host: Optional[str] = None  # noqa: UP007
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


def get_configs(proxystore_dir: str) -> list[EndpointConfig]:
    """Get all valid endpoint configurations in parent directory.

    Args:
        proxystore_dir: Parent directory containing possible endpoint
            configurations.

    Returns:
        List of found configs.
    """
    endpoints: list[EndpointConfig] = []

    if not os.path.isdir(proxystore_dir):
        return endpoints

    for dirpath, _, _ in os.walk(proxystore_dir):
        if os.path.samefile(proxystore_dir, dirpath):
            continue
        try:
            cfg = read_config(dirpath)
        except FileNotFoundError:
            continue
        except ValueError:
            continue
        else:
            endpoints.append(cfg)

    return endpoints


def get_log_filepath(endpoint_dir: str) -> str:
    """Return path to log file for endpoint.

    Args:
        endpoint_dir: Directory for the endpoint.

    Returns:
        Path to log file.
    """
    return os.path.join(endpoint_dir, ENDPOINT_LOG_FILE)


def get_pid_filepath(endpoint_dir: str) -> str:
    """Return path to PID file for endpoint.

    Args:
        endpoint_dir: Directory for the endpoint.

    Returns:
        Path to PID file.
    """
    return os.path.join(endpoint_dir, ENDPOINT_PID_FILE)


def read_config(endpoint_dir: str) -> EndpointConfig:
    """Read endpoint config file.

    Args:
        endpoint_dir: Directory containing endpoint configuration file.

    Returns:
        Config found in `endpoint_dir`.

    Raises:
        FileNotFoundError: If a config files does not exist in the directory.
        ValueError: If config contains an invalid value or cannot be parsed.
    """
    path = os.path.join(endpoint_dir, ENDPOINT_CONFIG_FILE)

    if os.path.exists(path):
        with open(path, 'rb') as f:
            try:
                return load(EndpointConfig, f)
            except Exception as e:
                raise ValueError(
                    f'Unable to parse ({path}): {e!s}.',
                ) from None
    else:
        raise FileNotFoundError(
            f'Endpoint directory {endpoint_dir} does not contain a valid '
            'configuration.',
        )


def validate_name(name: str) -> bool:
    """Validate name only contains alphanumeric or dash/underscore chars."""
    return len(re.findall(r'[^A-Za-z0-9_\-]', name)) == 0 and len(name) > 0


def write_config(cfg: EndpointConfig, endpoint_dir: str) -> None:
    """Write config to endpoint directory.

    Args:
        cfg: Configuration to write.
        endpoint_dir: Directory to write config to.
    """
    os.makedirs(endpoint_dir, exist_ok=True)
    path = os.path.join(endpoint_dir, ENDPOINT_CONFIG_FILE)
    with open(path, 'wb') as f:
        dump(cfg, f)

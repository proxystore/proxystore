"""Endpoint configuration."""
from __future__ import annotations

import dataclasses
import json
import os
import re
import uuid

from proxystore.endpoint.constants import MAX_OBJECT_SIZE_DEFAULT

ENDPOINT_CONFIG_FILE = 'config.json'
ENDPOINT_DATABASE_FILE = 'blobs.db'
ENDPOINT_LOG_FILE = 'log.txt'
ENDPOINT_PID_FILE = 'daemon.pid'


@dataclasses.dataclass
class EndpointConfig:
    """Endpoint configuration.

    Attributes:
        name: Endpoint name.
        uuid: Endpoint UUID.
        host: Host endpoint is running on.
        port: Port endpoint is running on.
        relay_server: Optional relay server the endpoint should register with.
        database_path: Optional path to SQLite database file that will be used
            for storing endpoint data. If `None`, data will only be stored
            in-memory.
        max_object_size: Optional maximum object size.
        peer_channels: Number of peer channels to multiplex communications
            over.
        verify_certificates: Validate the SSL certificates of the `relay`
            server.

    Raises:
        ValueError: If the name does not contain only alphanumeric, dash, or
            underscore characters, if the UUID cannot be parsed, or if the
            port is not in the range [1, 65535].
    """

    name: str
    uuid: uuid.UUID
    host: str | None
    port: int
    relay_server: str | None = None
    database_path: str | None = None
    max_object_size: int | None = MAX_OBJECT_SIZE_DEFAULT
    peer_channels: int = 1
    verify_certificate: bool = True

    def __post_init__(self) -> None:
        if not validate_name(self.name):
            raise ValueError(
                'Name must only contain alphanumeric characters, dashes, and '
                f' underscores. Got {self.name}.',
            )
        if isinstance(self.uuid, str):
            try:
                self.uuid = uuid.UUID(self.uuid, version=4)
            except ValueError:
                raise ValueError(
                    f'{self.uuid} is not a valid UUID4 string.',
                ) from None
        if self.port < 1 or self.port > 65535:
            raise ValueError('Port must be in range [1, 65535].')
        if self.relay_server is not None and not (
            self.relay_server.startswith('ws://')
            or self.relay_server.startswith('wss://')
        ):
            raise ValueError(
                'Server must start with ws:// or wss://.',
            )
        if self.max_object_size is not None and self.max_object_size < 1:
            raise ValueError(
                'Max object size must be None or greater than zero.',
            )
        if self.peer_channels < 1:
            raise ValueError('Peer channels must be >= 1.')


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
        with open(path) as f:
            try:
                cfg_json = json.load(f)
            except json.decoder.JSONDecodeError as e:
                raise ValueError(
                    f'Unable to parse ({path}): {e!s}.',
                ) from None
        try:
            cfg = EndpointConfig(**cfg_json)
        except TypeError as e:
            raise ValueError(
                f'Keys in config ({path}) do not match expected: {e!s}.',
            ) from None
        return cfg
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
    with open(path, 'w') as f:
        data = dataclasses.asdict(cfg)
        data['uuid'] = str(data['uuid'])
        json.dump(data, f, indent=4)
        # Add newline so cat on the file looks better
        f.write('\n')

"""Endpoint configuration."""
from __future__ import annotations

import dataclasses
import json
import os
import re
import uuid

from proxystore.endpoint.constants import MAX_OBJECT_SIZE_DEFAULT

_ENDPOINT_CONFIG_FILE = 'endpoint.json'
_ENDPOINT_LOG_FILE = 'endpoint.log'
_ENDPOINT_PID_FILE = 'daemon.pid'


@dataclasses.dataclass
class EndpointConfig:
    """Endpoint configuration."""

    name: str
    uuid: uuid.UUID
    host: str | None
    port: int
    server: str | None = None
    max_memory: int | None = None
    max_object_size: int | None = MAX_OBJECT_SIZE_DEFAULT
    dump_dir: str | None = None
    peer_channels: int = 1
    verify_certificate: bool = True

    def __post_init__(self) -> None:
        """Validate config contains reasonable values.

        Raises:
            ValueError:
                if the name does not contain only alphanumeric, dash, or
                underscore characters, if the UUID cannot be parsed, or if the
                port is not in the range [1, 65535].
        """
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
        if self.server is not None and not (
            self.server.startswith('ws://') or self.server.startswith('wss://')
        ):
            raise ValueError(
                'Server must start with ws:// or wss://.',
            )
        if self.max_memory is not None and self.max_memory < 1:
            raise ValueError('Max memory must be None or greater than zero.')
        if self.max_object_size is not None and self.max_object_size < 1:
            raise ValueError(
                'Max object size must be None or greater than zero.',
            )
        if self.peer_channels < 1:
            raise ValueError('Peer channels must be >= 1.')


def get_configs(proxystore_dir: str) -> list[EndpointConfig]:
    """Get all valid endpoint configurations in parent directory.

    Args:
        proxystore_dir (str): parent directory containing possible endpoint
            configurations.

    Returns:
        list of :class:`~proxystore.endpoint.config.EndpointConfig`.
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
        endpoint_dir (str): directory for the endpoint.

    Returns:
        path to log file.
    """
    return os.path.join(endpoint_dir, _ENDPOINT_LOG_FILE)


def get_pid_filepath(endpoint_dir: str) -> str:
    """Return path to PID file for endpoint.

    Args:
        endpoint_dir (str): directory for the endpoint.

    Returns:
        path to PID file.
    """
    return os.path.join(endpoint_dir, _ENDPOINT_PID_FILE)


def read_config(endpoint_dir: str) -> EndpointConfig:
    """Read endpoint config file.

    Args:
        endpoint_dir (str): directory containing endpoint configuration file.

    Returns:
        :class:`<.EndpointConfig>`

    Raises:
        FileNotFoundError:
            if a config files does not exist in the directory.
        ValueError:
            if config contains an invalid value or cannot be parsed.
    """
    path = os.path.join(endpoint_dir, _ENDPOINT_CONFIG_FILE)

    if os.path.exists(path):
        with open(path) as f:
            try:
                cfg_json = json.load(f)
            except json.decoder.JSONDecodeError as e:
                raise ValueError(
                    f'Unable to parse ({path}): {str(e)}.',
                ) from None
        try:
            cfg = EndpointConfig(**cfg_json)
        except TypeError as e:
            raise ValueError(
                f'Keys in config ({path}) do not match expected: {str(e)}.',
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
        cfg (EndpointConfig): configuration to write.
        endpoint_dir (str): directory to write config to.
    """
    os.makedirs(endpoint_dir, exist_ok=True)
    path = os.path.join(endpoint_dir, _ENDPOINT_CONFIG_FILE)
    with open(path, 'w') as f:
        data = dataclasses.asdict(cfg)
        data['uuid'] = str(data['uuid'])
        json.dump(data, f, indent=4)
        # Add newline so cat on the file looks better
        f.write('\n')

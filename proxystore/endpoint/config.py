"""Endpoint config utilities."""
from __future__ import annotations

import dataclasses
import json
import os
import pathlib

_DEFAULT_HOME_DIR = '.proxystore'
_ENDPOINT_CONFIG_FILE = 'endpoint.json'


@dataclasses.dataclass
class EndpointConfig:
    """Endpoint configuration."""

    name: str
    uuid: str
    host: str
    port: int
    server: str | None = None


def default_dir() -> str:
    """Returns path of $HOME/.proxystore."""
    return os.path.join(pathlib.Path.home(), _DEFAULT_HOME_DIR)


def get_configs(proxystore_dir: str) -> list[EndpointConfig]:
    """Get all endpoint configurations in parent directory.

    Args:
        proxystore_dir (str): parent directory containing possible endpoint
            configurations.

    Returns:
        list of :class:`<.EndpointConfig>`s
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
        else:
            endpoints.append(cfg)

    return endpoints


def read_config(endpoint_dir: str) -> EndpointConfig:
    """Read endpoint config file.

    Args:
        endpoint_dir (str): directory containing endpoint configuration file.

    Returns:
        :class:`<.EndpointConfig>`

    Raises:
        FileNotFoundError:
            if a config files does not exist in the directory.
    """
    path = os.path.join(endpoint_dir, _ENDPOINT_CONFIG_FILE)

    if os.path.exists(path):
        with open(path) as f:
            cfg = json.load(f)
        return EndpointConfig(**cfg)
    else:
        raise FileNotFoundError(
            f'Endpoint directory {endpoint_dir} does not contain a valid '
            'configuration.',
        )


def write_config(cfg: EndpointConfig, endpoint_dir: str) -> None:
    """Write config to endpoint directory.

    Args:
        cfg (EndpointConfig): configuration to write.
        endpoint_dir (str): directory to write config to.
    """
    os.makedirs(endpoint_dir, exist_ok=True)
    path = os.path.join(endpoint_dir, _ENDPOINT_CONFIG_FILE)
    with open(path, 'w') as f:
        json.dump(dataclasses.asdict(cfg), f, indent=4)
        # Add newline so cat on the file looks better
        f.write('\n')

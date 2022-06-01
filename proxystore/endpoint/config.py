"""Endpoint config utilities."""
from __future__ import annotations

import dataclasses
import json
import os
import pathlib
from typing import Any

_DEFAULT_HOME_DIR = '.proxystore'
_ENDPOINT_CONFIG_FILE = 'endpoint.json'


@dataclasses.dataclass
class EndpointConfig:
    """Endpoint configuration."""

    name: str | None = None
    uuid: str | None = None
    host: str | None = None
    port: int | None = None


def default_dir() -> str:
    """Returns path of $HOME/.proxystore."""
    return os.path.join(pathlib.Path.home(), _DEFAULT_HOME_DIR)


def get_config(endpoint_dir: str) -> EndpointConfig:
    """Get existing config from path or create new one."""
    path = os.path.join(endpoint_dir, _ENDPOINT_CONFIG_FILE)

    if os.path.exists(path):
        with open(path) as f:
            cfg = json.load(f)
        return EndpointConfig(**cfg)
    else:
        return EndpointConfig()


def save_config(cfg: EndpointConfig, endpoint_dir: str) -> None:
    """Write config to path."""
    os.makedirs(endpoint_dir, exist_ok=True)
    path = os.path.join(endpoint_dir, _ENDPOINT_CONFIG_FILE)
    with open(path, 'w') as f:
        json.dump(dataclasses.asdict(cfg), f)


def update_config(endpoint_dir: str, **kwargs: Any) -> None:
    """Update config."""
    cfg = get_config(endpoint_dir)
    for key, value in kwargs.items():
        if hasattr(cfg, key):
            setattr(cfg, key, value)
        else:
            raise AttributeError(
                f'{EndpointConfig.__name__} has no attribute {key}.',
            )
    save_config(cfg, endpoint_dir)

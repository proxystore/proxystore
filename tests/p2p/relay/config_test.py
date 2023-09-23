from __future__ import annotations

import logging
import pathlib

from proxystore.p2p.relay.config import RelayAuthConfig
from proxystore.p2p.relay.config import RelayLoggingConfig
from proxystore.p2p.relay.config import RelayServingConfig


def test_auth_config_default() -> None:
    config = RelayAuthConfig()
    assert config.method is None


def test_logging_config_default() -> None:
    config = RelayLoggingConfig()
    assert config.default_level == logging.INFO


def test_read_from_config_file_empty(tmp_path: pathlib.Path) -> None:
    data = '[serving]'

    filepath = tmp_path / 'relay.toml'
    with open(filepath, 'w') as f:
        f.write(data)

    config = RelayServingConfig.from_toml(filepath)
    assert config == RelayServingConfig()


def test_read_from_config_file(tmp_path: pathlib.Path) -> None:
    data = """\
host = "localhost"
port = 1234
certfile = "/path/to/cert.pem"
keyfile = "/path/to/privkey.pem"

[auth]
method = "globus"

[auth.kwargs]
client_id = "ABC"

[logging]
log_dir = "/path/to/log/dir"
default_level = "DEBUG"
websockets_level = "INFO"
current_client_interval = 3
current_client_limit = 5
"""

    filepath = tmp_path / 'relay.toml'
    with open(filepath, 'w') as f:
        f.write(data)

    config = RelayServingConfig.from_toml(filepath)

    assert config.host == 'localhost'
    assert config.port == 1234
    assert config.certfile == '/path/to/cert.pem'
    assert config.keyfile == '/path/to/privkey.pem'

    assert config.auth.method == 'globus'
    assert config.auth.kwargs['client_id'] == 'ABC'
    assert 'client_secret' not in config.auth.kwargs

    assert config.logging.log_dir == '/path/to/log/dir'
    assert config.logging.default_level == 'DEBUG'
    assert config.logging.websockets_level == 'INFO'
    assert config.logging.current_client_interval == 3
    assert config.logging.current_client_limit == 5

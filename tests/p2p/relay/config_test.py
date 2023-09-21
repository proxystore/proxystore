from __future__ import annotations

import logging
import pathlib

import pytest

from proxystore.p2p.relay.config import RelayAuthConfig
from proxystore.p2p.relay.config import RelayLoggingConfig
from proxystore.p2p.relay.config import RelayServingConfig


def test_auth_config_default() -> None:
    config = RelayAuthConfig()
    assert config.method is None


def test_auth_config_from_config_dict() -> None:
    config = RelayAuthConfig.from_config_dict({})
    assert config == RelayAuthConfig()

    # Check method case parsing
    config = RelayAuthConfig.from_config_dict({'method': 'GLOBUS'})
    assert config.method == 'globus'

    # Check extra kwargs
    config = RelayAuthConfig.from_config_dict(
        {'method': 'GLOBUS', 'kwarg1': 'value'},
    )
    assert config.method == 'globus'
    assert config.kwargs['kwarg1'] == 'value'

    # Check method unknown error
    with pytest.raises(ValueError, match='Unknown authentication method'):
        RelayAuthConfig.from_config_dict({'method': 'fake'})


def test_logging_config_default() -> None:
    config = RelayLoggingConfig()
    assert config.default_level == logging.INFO


def test_logging_config_from_config_dict() -> None:
    config = RelayLoggingConfig.from_config_dict({})
    assert config == RelayLoggingConfig()

    # Check type conversion using string versions of default options
    options = {
        'default_level': 'INFO',
        'websockets_level': 'WARNING',
        'current_client_interval': str(config.current_client_interval),
        'current_client_limit': str(config.current_client_limit),
    }
    config = RelayLoggingConfig.from_config_dict(options)
    assert config == RelayLoggingConfig()


def test_read_from_config_file_empty(tmp_path: pathlib.Path) -> None:
    data = '[serving]'

    filepath = tmp_path / 'relay.cfg'
    with open(filepath, 'w') as f:
        f.write(data)

    config = RelayServingConfig.from_config(filepath)
    assert config == RelayServingConfig()


def test_read_from_config_file(tmp_path: pathlib.Path) -> None:
    data = """\
[serving]
host = localhost
port = 1234
certfile = /path/to/cert.pem
keyfile = /path/to/privkey.pem

[serving.auth]
method = globus
client_id = ABC
client_secret

[serving.logging]
log_dir = /path/to/log/dir
default_level = DEBUG
websockets_level = INFO
current_client_interval = 3
current_client_limit = 5
"""

    filepath = tmp_path / 'relay.cfg'
    with open(filepath, 'w') as f:
        f.write(data)

    config = RelayServingConfig.from_config(filepath)

    assert config.host == 'localhost'
    assert config.port == 1234
    assert config.certfile == '/path/to/cert.pem'
    assert config.keyfile == '/path/to/privkey.pem'

    assert config.auth.method == 'globus'
    assert config.auth.kwargs['client_id'] == 'ABC'
    assert config.auth.kwargs['client_secret'] is None

    assert config.logging.log_dir == '/path/to/log/dir'
    assert config.logging.default_level == logging.DEBUG
    assert config.logging.websockets_level == logging.INFO
    assert config.logging.current_client_interval == 3
    assert config.logging.current_client_limit == 5

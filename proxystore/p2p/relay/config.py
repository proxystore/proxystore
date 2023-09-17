"""Relay server configuration file parsing."""
from __future__ import annotations

import configparser
import dataclasses
import logging
import pathlib
import sys
from typing import Any
from typing import Literal

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


@dataclasses.dataclass
class RelayAuthConfig:
    """Relay authentication configuration.

    Attributes:
        method: Authentication method.
    """

    method: Literal['globus'] | None = None
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_config_dict(cls, options: dict[str, str]) -> Self:
        """Parse authentication configuration from dictionary of strings.

        Args:
            options: Flat dictionary mapping of string keys to string values
                to be parsed into the correct types.

        Returns:
            Authentication configuration.
        """
        method = options.pop('method', None)
        if isinstance(method, str):
            method = method.lower()
            if method not in ('globus',):
                raise ValueError(f'Unknown authentication method "{method}".')
        return cls(method, options)  # type: ignore[arg-type]


@dataclasses.dataclass
class RelayLoggingConfig:
    """Relay logging configuration.

    Attributes:
        log_dir: Default logging directory.
        default_level: Default logging level for the root logger.
        websockets_level: Log level for the `websockets` logger. Websockets
            logs with much higher frequency so it is suggested to set this
            to `WARNING` or higher.
        current_clients_interval: Optional seconds between logging the
            number of currently connected clients and user.
        current_client_limit: Max threshold for enumerating the
            detailed list of connected clients. If `None`, no detailed
            list will be logged.
    """

    log_dir: str | None = None
    default_level: int = logging.INFO
    websockets_level: int = logging.WARNING
    current_client_interval: int | None = 60
    current_client_limit: int | None = 32

    @classmethod
    def from_config_dict(cls, options: dict[str, str]) -> Self:
        """Parse logging configuration from dictionary of strings.

        Args:
            options: Flat dictionary mapping of string keys to string values
                to be parsed into the correct types.

        Returns:
            Logging configuration.
        """
        processed_options: dict[str, Any] = {}
        processed_options['log_dir'] = options.get('log_dir', None)

        default_level = options.get('default_level', None)
        if default_level is not None:
            processed_options['default_level'] = logging.getLevelName(
                default_level,
            )

        websockets_level = options.get('websockets_level', None)
        if websockets_level is not None:
            processed_options['websockets_level'] = logging.getLevelName(
                websockets_level,
            )

        client_interval = options.get('current_client_interval', None)
        if client_interval is not None:
            processed_options['current_client_interval'] = int(client_interval)

        client_limit = options.get('current_client_limit', None)
        if client_limit is not None:
            processed_options['current_client_limit'] = int(client_limit)

        return cls(**processed_options)


@dataclasses.dataclass
class RelayServingConfig:
    """Relay serving configuration.

    Attributes:
        host: Network interface the server binds to.
        port: Network port the server binds to.
        certfile: Certificate file (PEM format) use to enable TLS.
        keyfile: Private key file. If not specified, the key will be
            taken from the certfile.
        auth: Authentication configuration.
        logging: Logging configuration.
    """

    host: str | None = None
    port: int = 8700
    certfile: str | None = None
    keyfile: str | None = None
    auth: RelayAuthConfig = dataclasses.field(default_factory=RelayAuthConfig)
    logging: RelayLoggingConfig = dataclasses.field(
        default_factory=RelayLoggingConfig,
    )

    @classmethod
    def from_config(cls, filepath: str | pathlib.Path) -> Self:
        """Parse an INI config file.

        Example:
            Minimal config without SSL and without authentication.
            ```cfg title="relay.cfg"
            [serving]
            port = 8700

            [serving.logging]
            log_dir = /path/to/log/dir
            default_log_level = INFO
            websockets_log_level = WARNING
            connected_client_logging_interval = 60
            connected_client_logging_limit = 32
            ```

            ```python
            from proxystore.p2p.relay.globus.config

            config = RelayServingConfig.from_config('relay.cfg')
            ```

        Example:
            Serve with SSL and Globus Auth.
            ```cfg title="relay.cfg"
            [serving]
            host = 0.0.0.0
            port = 8700
            certfile = /path/to/cert.pem
            keyfile = /path/to/privkey.pem

            [serving.auth]
            method = 'globus'
            client_id = ...
            client_secret = ...
            audience = ...

            [serving.logging]
            log_dir = /path/to/log/dir
            default_log_level = INFO
            websockets_log_level = WARNING
            connected_client_logging_interval = 60
            connected_client_logging_limit = 32
            ```

        Note:
            Omitted values will be set to their defaults (if they are an
            optional value with a default), and options without values
            are considered `None`. E.g.,
            ```cfg title="relay.cfg"
            [serving]
            ...
            certfile = /path/to/cert.pem
            keyfile
            ```

            ```python
            from proxystore.p2p.relay.config import RelayServingConfig

            config = RelayServingConfig.from_config('relay.cfg')
            assert config.certfile == '/path/to/cert.pem'
            assert config.keyfile is None
            ```
        """
        config = configparser.ConfigParser(allow_no_value=True)
        with open(filepath) as f:
            config.read_string(f.read())

        options: dict[str, Any] = (
            dict(config['serving']) if 'serving' in config else {}
        )
        # Currently every value type in options is str | None, but some
        # options need to be cast to ints
        if 'port' in options and options['port'] is not None:
            options['port'] = int(options['port'])

        auth_options = (
            config['serving.auth'] if 'serving.auth' in config else None
        )
        if auth_options is not None:
            options['auth'] = RelayAuthConfig.from_config_dict(
                dict(auth_options),
            )

        logging_options = (
            config['serving.logging'] if 'serving.logging' in config else None
        )
        if logging_options is not None:
            options['logging'] = RelayLoggingConfig.from_config_dict(
                dict(logging_options),
            )

        return cls(**options)

"""Relay server configuration file parsing."""
from __future__ import annotations

import logging
import pathlib
import sys
from typing import Any
from typing import Dict
from typing import Literal
from typing import Optional
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import Field

from proxystore.utils.config import load


class RelayAuthConfig(BaseModel):
    """Relay authentication configuration.

    Attributes:
        method: Authentication method.
        kwargs: Arbitrary keyword arguments to pass to the authenticator.
            The kwargs are excluded from the [`repr()`][repr] of this
            class because they often contain secrets.
    """

    method: Optional[Literal['globus']] = None  # noqa: UP007
    kwargs: Dict[str, Any] = Field(  # noqa: UP006
        default_factory=dict,
        repr=False,
    )


class RelayLoggingConfig(BaseModel):
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

    log_dir: Optional[str] = None  # noqa: UP007
    default_level: Union[int, str] = logging.INFO  # noqa: UP007
    websockets_level: Union[int, str] = logging.WARNING  # noqa: UP007
    current_client_interval: Optional[int] = 60  # noqa: UP007
    current_client_limit: Optional[int] = 32  # noqa: UP007


class RelayServingConfig(BaseModel):
    """Relay serving configuration.

    Attributes:
        host: Network interface the server binds to.
        port: Network port the server binds to.
        certfile: Certificate file (PEM format) use to enable TLS.
        keyfile: Private key file. If not specified, the key will be
            taken from the certfile.
        auth: Authentication configuration.
        logging: Logging configuration.
        max_message_bytes: Maximum size in bytes of messages received by
            the relay server.
    """

    host: Optional[str] = None  # noqa: UP007
    port: int = 8700
    certfile: Optional[str] = None  # noqa: UP007
    keyfile: Optional[str] = None  # noqa: UP007
    auth: RelayAuthConfig = Field(default_factory=RelayAuthConfig)
    logging: RelayLoggingConfig = Field(default_factory=RelayLoggingConfig)
    max_message_bytes: Optional[int] = None  # noqa: UP007

    @classmethod
    def from_toml(cls, filepath: str | pathlib.Path) -> Self:
        """Parse an TOML config file.

        Example:
            Minimal config without SSL and without authentication.
            ```toml title="relay.toml"
            port = 8700

            [logging]
            log_dir = "/path/to/log/dir"
            default_log_level = "INFO"
            websockets_log_level = "WARNING"
            connected_client_logging_interval = 60
            connected_client_logging_limit = 32
            ```

            ```python
            from proxystore.p2p.relay.globus.config

            config = RelayServingConfig.from_toml('relay.toml')
            ```

        Example:
            Serve with SSL and Globus Auth.
            ```toml title="relay.toml"
            host = "0.0.0.0"
            port = 8700
            certfile = "/path/to/cert.pem"
            keyfile = "/path/to/privkey.pem"

            [auth]
            method = "globus"

            [auth.kwargs]
            client_id = "..."
            client_secret = "..."

            [logging]
            log_dir = "/path/to/log/dir"
            default_log_level = "INFO"
            websockets_log_level = "WARNING"
            connected_client_logging_interval = 60
            connected_client_logging_limit = 32
            ```

        Note:
            Omitted values will be set to their defaults (if they are an
            optional value with a default).
            ```toml title="relay.toml"
            [serving]
            certfile = "/path/to/cert.pem"
            ```

            ```python
            from proxystore.p2p.relay.config import RelayServingConfig

            config = RelayServingConfig.from_config('relay.toml')
            assert config.certfile == '/path/to/cert.pem'
            assert config.keyfile is None
            ```
        """
        with open(filepath, 'rb') as f:
            return load(cls, f)

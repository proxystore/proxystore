"""Endpoint directory layout and files."""

from __future__ import annotations

import contextlib
import dataclasses
import enum
import logging
import os
import random
import shutil
import stat
from typing import Any
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator

from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.files import check_format_version
from proxystore.endpoint.files import read_json_model
from proxystore.endpoint.files import write_json_model
from proxystore.endpoint.files import write_private_file
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.peers import Peers
from proxystore.utils.config import dump
from proxystore.utils.config import load
from proxystore.utils.environment import home_dir

logger = logging.getLogger(__name__)

CONNECTION_VERSION = 1
"""Format version of the connection file."""


class ConnectionInfo(BaseModel):
    """Information that clients use to connect to a running endpoint.

    The endpoint writes this to the connection file in its directory each
    time it starts and removes it when it stops (see
    [`EndpointDir.write_connection()`][proxystore.endpoint.directory.EndpointDir.write_connection]).

    Attributes:
        version: Format version of the connection file.
        host: Host address the endpoint is listening on.
        port: Port the endpoint is listening on.
        token: Token that the client and endpoint prove they know.
        tls_fingerprint: SHA-256 fingerprint of the endpoint's TLS
            certificate or `None` if the endpoint does not use TLS.
        hostname: Name of the machine the endpoint is running on.
        pid: Process ID of the endpoint on that machine.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    version: int = CONNECTION_VERSION
    host: str
    port: int
    token: EndpointToken
    tls_fingerprint: str | None
    hostname: str
    pid: int

    @field_validator('version')
    @classmethod
    def _version_validator(cls, v: int) -> int:
        return check_format_version(v, CONNECTION_VERSION, 'connection file')


class EndpointStatus(enum.Enum):
    """Endpoint status."""

    RUNNING = enum.auto()
    """Endpoint is running on this host."""
    STOPPED = enum.auto()
    """Endpoint is stopped."""
    UNKNOWN = enum.auto()
    """Endpoint cannot be found (missing/corrupted directory)."""
    HANGING = enum.auto()
    """Endpoint PID file exists but process is not active.

    This is either because the process died unexpectedly or the endpoint
    is running on another host.
    """


@dataclasses.dataclass(frozen=True)
class EndpointDir:
    """Directory of an endpoint.

    An endpoint directory contains the endpoint's configuration and the
    files created while it runs (e.g., its log and the connection file that
    clients use to connect).

    Example:
        ```python
        # Create a new endpoint with a new secret key
        endpoint_dir = EndpointDir.create('my-ep', port=8765)
        # Or open an existing endpoint
        endpoint_dir = EndpointDir.from_name('my-ep')
        config = endpoint_dir.read_config()
        ```

    Attributes:
        path: Path of the directory.
    """

    path: str

    def __fspath__(self) -> str:
        return self.path

    def __str__(self) -> str:
        return self.path

    @classmethod
    def from_name(
        cls,
        name: str,
        proxystore_dir: str | None = None,
    ) -> Self:
        """Get the directory of an endpoint in a ProxyStore home directory.

        The directory may not exist (e.g., because the endpoint has not been
        created yet).

        Args:
            name: Name of the endpoint.
            proxystore_dir: ProxyStore home directory. Defaults to
                [`home_dir()`][proxystore.utils.environment.home_dir].
        """
        return cls(os.path.join(resolve_home(proxystore_dir), name))

    @classmethod
    def create(
        cls,
        name: str,
        proxystore_dir: str | None = None,
        *,
        secret_key: SecretKey | None = None,
        port: int | None = None,
        **options: Any,
    ) -> Self:
        """Create a new endpoint.

        Creates the endpoint directory, only accessible by the owner, and
        writes a new secret key and the configuration of the endpoint.

        Example:
            ```python
            endpoint_dir = EndpointDir.create(
                'my-ep',
                port=8765,
                p2p=EndpointP2PConfig(relays='none'),
            )
            ```

        Args:
            name: Name of the endpoint.
            proxystore_dir: ProxyStore home directory. Defaults to
                [`home_dir()`][proxystore.utils.environment.home_dir].
            secret_key: Secret key of the endpoint. A new key is generated
                if `None`.
            port: Port of the endpoint. A random port in the range
                [10240, 20480] is chosen if `None`.
            options: Other fields of the
                [`EndpointConfig`][proxystore.endpoint.config.EndpointConfig]
                (e.g., `port`). The `name` and `id` are set automatically.

        Returns:
            The new endpoint directory.

        Raises:
            EndpointExistsError: If an endpoint with the name already exists.
            ValueError: If the configuration is invalid.
        """
        secret_key = SecretKey.generate() if secret_key is None else secret_key
        port = random.randint(10 * 1024, 20 * 1024) if port is None else port
        config = EndpointConfig(
            name=name,
            id=secret_key.endpoint_id,
            port=port,
            **options,
        )
        endpoint_dir = cls.from_name(name, proxystore_dir)
        os.makedirs(os.path.dirname(endpoint_dir.path), exist_ok=True)
        # Clients trust the files in the endpoint directory, so only the
        # owner can create or replace files in it.
        try:
            os.mkdir(endpoint_dir.path, mode=0o700)
        except FileExistsError:
            raise EndpointExistsError(
                f'An endpoint named {name} already exists in '
                f'{os.path.dirname(endpoint_dir.path)}.',
            ) from None
        endpoint_dir.write_secret_key(secret_key)
        # The configuration is written last because an endpoint is only
        # found (see find_all()) once it has a configuration.
        endpoint_dir.write_config(config)
        return endpoint_dir

    @classmethod
    def find_all(
        cls,
        proxystore_dir: str | None = None,
    ) -> list[tuple[Self, EndpointConfig]]:
        """Find all endpoints with a valid configuration.

        Args:
            proxystore_dir: ProxyStore home directory to search in. Defaults
                to [`home_dir()`][proxystore.utils.environment.home_dir].

        Returns:
            List of each endpoint directory and its configuration.
        """
        proxystore_dir = resolve_home(proxystore_dir)
        endpoints: list[tuple[Self, EndpointConfig]] = []
        if not os.path.isdir(proxystore_dir):
            return endpoints

        # Endpoint directories are always direct children of the home
        # directory (see from_name()).
        with os.scandir(proxystore_dir) as entries:
            paths = sorted(entry.path for entry in entries if entry.is_dir())

        for path in paths:
            endpoint_dir = cls(path)
            try:
                config = endpoint_dir.read_config()
            except (FileNotFoundError, ValueError):
                continue
            endpoints.append((endpoint_dir, config))

        return endpoints

    def read_config(self) -> EndpointConfig:
        """Read the endpoint configuration.

        Raises:
            EndpointNotFoundError: If the configuration file does not exist.
            EndpointConfigError: If the configuration contains an invalid
                value, cannot be parsed, or the name of the endpoint does
                not match the name of the directory.
        """
        try:
            with open(self.config_path, 'rb') as f:
                config = load(EndpointConfig, f)
        except FileNotFoundError:
            if not os.path.isdir(self.path):
                raise EndpointNotFoundError(
                    f'An endpoint named {os.path.basename(self.path)} does '
                    f'not exist in {os.path.dirname(self.path)}.',
                ) from None
            raise EndpointNotFoundError(
                f'Endpoint directory {self.path} does not contain a valid '
                'configuration.',
            ) from None
        except ValueError as e:
            # Includes TOML decoding and pydantic validation errors.
            raise EndpointConfigError(
                f'Unable to parse ({self.config_path}): {e!s}.',
            ) from None

        # The directory name is used to find an endpoint by name, so the
        # name in the configuration must match it.
        dir_name = os.path.basename(os.path.normpath(self.path))
        if config.name != dir_name:
            raise EndpointConfigError(
                f'The endpoint name in {self.config_path} ({config.name}) '
                f'does not match the name of the directory ({dir_name}). '
                'Rename the directory or change the name in the '
                'configuration so they match.',
            )
        return config

    def write_config(self, config: EndpointConfig) -> None:
        """Write the endpoint configuration, creating the directory if needed.

        Args:
            config: Configuration to write.
        """
        # Clients trust the connection file in the endpoint directory, so
        # only the owner can create or replace files in it.
        os.makedirs(self.path, mode=0o700, exist_ok=True)
        with open(self.config_path, 'wb') as f:
            dump(config, f)

    @property
    def config_path(self) -> str:
        """Path to the endpoint configuration."""
        return self._join('config.toml')

    def resolve_path(self, path: str) -> str:
        """Resolve a path in the configuration of the endpoint.

        `~` is expanded to the user's home directory, and a relative path
        is relative to the endpoint directory.
        """
        path = os.path.expanduser(path)
        return path if os.path.isabs(path) else self._join(path)

    @property
    def log_path(self) -> str:
        """Path to the log of the endpoint daemon."""
        return self._join('log.txt')

    @property
    def pid_path(self) -> str:
        """Path to the PID file of the endpoint daemon."""
        return self._join('daemon.pid')

    @property
    def secret_key_path(self) -> str:
        """Path to the secret key of the endpoint."""
        return self._join('secret.key')

    def write_secret_key(self, secret_key: SecretKey) -> None:
        """Atomically write the secret key of the endpoint.

        The file is only readable by the owner.
        """
        write_private_file(self.secret_key_path, secret_key.to_bytes())

    def read_secret_key(
        self,
        endpoint_id: EndpointId | None = None,
    ) -> SecretKey:
        """Read the secret key of the endpoint.

        Args:
            endpoint_id: ID the secret key must match. If `None`, the key is
                checked against the ID in the configuration of the endpoint,
                if the configuration exists.

        Raises:
            EndpointConfigError: If the secret key file does not exist, is
                malformed, or does not match the ID.
        """
        try:
            with open(self.secret_key_path, 'rb') as f:
                data = f.read()
        except FileNotFoundError:
            raise EndpointConfigError(
                f'Endpoint directory {self.path} does not contain a secret '
                'key. Remove the endpoint and configure it again with '
                '"proxystore-endpoint configure".',
            ) from None
        try:
            secret_key = SecretKey(data)
        except ValueError:
            raise EndpointConfigError(
                f'Secret key file at {self.secret_key_path} is malformed.',
            ) from None

        if endpoint_id is None and os.path.exists(self.config_path):
            endpoint_id = self.read_config().id
        if endpoint_id is not None and secret_key.endpoint_id != endpoint_id:
            raise EndpointConfigError(
                f'The endpoint ID in the configuration ({endpoint_id}) '
                'does not match the secret key '
                f'({secret_key.endpoint_id}) in {self.path}.',
            )
        return secret_key

    @property
    def peers_path(self) -> str:
        """Path to the allowlist of peer endpoints."""
        return self._join('peers.toml')

    @property
    def peers(self) -> Peers:
        """Peers of the endpoint.

        Raises:
            FileNotFoundError: If the configuration does not exist.
            ValueError: If the configuration is invalid.
        """
        return Peers(self.peers_path, owner_id=self.read_config().id)

    @property
    def peer_addrs_path(self) -> str:
        """Path to the cache of peer addresses written by the endpoint."""
        return self._join('peer-addrs.json')

    @property
    def connection_path(self) -> str:
        """Path to the connection file clients use to connect."""
        return self._join('connection.json')

    def write_connection(self, info: ConnectionInfo) -> None:
        """Atomically write the connection file of the running endpoint.

        The file is only readable by the owner because it contains the
        endpoint's token.
        """
        write_json_model(self.connection_path, info)

    def read_connection(self) -> ConnectionInfo:
        """Read the connection file of the running endpoint.

        Raises:
            FileNotFoundError: If the connection file does not exist (e.g.,
                because the endpoint is not running).
            EndpointConfigError: If the connection file is malformed or has
                an unsupported format version.
        """
        return read_json_model(
            ConnectionInfo,
            self.connection_path,
            'connection file',
        )

    def remove_connection(self, info: ConnectionInfo | None = None) -> None:
        """Remove the connection file if it exists.

        Args:
            info: Only remove the connection file if it contains this
                information (i.e., it was not replaced by another instance
                of the endpoint).
        """
        if info is not None:
            try:
                if self.read_connection() != info:
                    return
            except (FileNotFoundError, ValueError):
                return
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.connection_path)

    def running_pid(self) -> int | None:
        """Get the PID of the endpoint daemon if it is running.

        Returns:
            The PID in the PID file if that process is running as the \
            current user on this host, otherwise `None` (e.g., the PID file \
            is missing or malformed, the endpoint stopped unexpectedly, or \
            the endpoint is running on a different host).
        """
        try:
            with open(self.pid_path) as f:
                pid = int(f.read().strip())
        except (OSError, ValueError):
            return None
        return pid if is_own_process(pid) else None

    def status(self) -> EndpointStatus:
        """Get the status of the endpoint.

        Returns:
            `EndpointStatus.RUNNING` if the endpoint has a valid \
            configuration and the PID file points to a running process. \
            `EndpointStatus.STOPPED` if the endpoint has a valid \
            configuration and no PID file. \
            `EndpointStatus.UNKNOWN` if the directory or configuration is \
            missing or invalid. \
            `EndpointStatus.HANGING` if the endpoint has a valid \
            configuration but the PID file does not point to a running \
            process. This can be due to the endpoint process dying \
            unexpectedly or the endpoint process is on a different host.
        """
        if not os.path.isdir(self.path):
            return EndpointStatus.UNKNOWN
        try:
            self.read_config()
        except (FileNotFoundError, ValueError) as e:
            logger.error(e)
            return EndpointStatus.UNKNOWN
        if not os.path.isfile(self.pid_path):
            return EndpointStatus.STOPPED
        if self.running_pid() is not None:
            return EndpointStatus.RUNNING
        return EndpointStatus.HANGING

    def remove(self) -> None:
        """Remove the endpoint directory and all of its files.

        Raises:
            EndpointNotFoundError: If the endpoint directory does not exist.
            EndpointRunningError: If the endpoint is running or its PID file
                exists (e.g., because it is running on another host).
        """
        if not os.path.isdir(self.path):
            raise EndpointNotFoundError(
                f'An endpoint named {os.path.basename(self.path)} does not '
                f'exist in {os.path.dirname(self.path)}.',
            )
        if self.status() in (EndpointStatus.RUNNING, EndpointStatus.HANGING):
            raise EndpointRunningError(
                f'Endpoint {os.path.basename(self.path)} must be stopped '
                'before it is removed.',
            )
        shutil.rmtree(self.path)

    def restrict_permissions(self) -> bool:
        """Remove all group and other permissions from the directory.

        Clients trust the connection file in the endpoint directory, so no
        one other than the owner may be able to create, replace, or rename
        files in it. The directory also contains the endpoint's secret key,
        database, and log which may contain user data. Group and other
        permissions are also removed from the secret key file.

        Returns:
            `True` if the permissions of the directory or secret key file \
            were changed.
        """
        changed = False
        for path in (self.path, self.secret_key_path):
            try:
                mode = stat.S_IMODE(os.stat(path).st_mode)
            except FileNotFoundError:
                continue
            if mode & 0o077 != 0:
                os.chmod(path, mode & ~0o077)
                changed = True
        return changed

    def _join(self, name: str) -> str:
        return os.path.join(self.path, name)


def resolve_home(proxystore_dir: str | None = None) -> str:
    """Resolve the ProxyStore home directory.

    Args:
        proxystore_dir: ProxyStore home directory. If `None`, the default
            [`home_dir()`][proxystore.utils.environment.home_dir] is used.
    """
    return home_dir() if proxystore_dir is None else proxystore_dir


def is_own_process(pid: int) -> bool:
    """Check if a process with the PID exists and is owned by this user."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        # PermissionError means the PID belongs to another user. The endpoint
        # always runs as the current user, so the endpoint exited and its PID
        # was reused by the OS.
        return False
    return True

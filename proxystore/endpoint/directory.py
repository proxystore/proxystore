"""Endpoint directory layout and files."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import stat
from typing import Self

from proxystore.endpoint.auth import ConnectionInfo
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.auth import write_private_file
from proxystore.endpoint.config import EndpointConfig
from proxystore.utils.config import dump
from proxystore.utils.config import load


@dataclasses.dataclass(frozen=True)
class EndpointDir:
    """Directory of an endpoint.

    An endpoint directory contains the endpoint's configuration and the
    files created while it runs (e.g., its log and the connection file that
    clients use to connect).

    Example:
        ```python
        endpoint_dir = EndpointDir.from_home('/path/to/proxystore', 'my-ep')
        assert endpoint_dir.path == '/path/to/proxystore/my-ep'
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
    def from_home(cls, proxystore_dir: str, name: str) -> Self:
        """Get the directory of an endpoint in a ProxyStore home directory.

        Args:
            proxystore_dir: ProxyStore home directory (see
                [`home_dir()`][proxystore.utils.environment.home_dir]).
            name: Name of the endpoint.
        """
        return cls(os.path.join(proxystore_dir, name))

    @classmethod
    def find_all(
        cls, proxystore_dir: str
    ) -> list[tuple[Self, EndpointConfig]]:
        """Find all endpoints with a valid configuration.

        Args:
            proxystore_dir: ProxyStore home directory to search in (see
                [`home_dir()`][proxystore.utils.environment.home_dir]).

        Returns:
            List of each endpoint directory and its configuration.
        """
        endpoints: list[tuple[Self, EndpointConfig]] = []
        if not os.path.isdir(proxystore_dir):
            return endpoints

        # Endpoint directories are always direct children of the home
        # directory (see from_home()).
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
            FileNotFoundError: If the configuration file does not exist.
            ValueError: If the configuration contains an invalid value or
                cannot be parsed.
        """
        try:
            with open(self.config_path, 'rb') as f:
                return load(EndpointConfig, f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f'Endpoint directory {self.path} does not contain a valid '
                'configuration.',
            ) from None
        except ValueError as e:
            # Includes TOML decoding and pydantic validation errors.
            raise ValueError(
                f'Unable to parse ({self.config_path}): {e!s}.',
            ) from None

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

    @property
    def database_path(self) -> str:
        """Path to the default SQLite database for persisting objects."""
        return self._join('blobs.db')

    @property
    def log_path(self) -> str:
        """Path to the log of the endpoint daemon."""
        return self._join('log.txt')

    @property
    def pid_path(self) -> str:
        """Path to the PID file of the endpoint daemon."""
        return self._join('daemon.pid')

    @property
    def connection_path(self) -> str:
        """Path to the connection file clients use to connect."""
        return self._join('connection.json')

    def write_connection(self, info: ConnectionInfo) -> None:
        """Atomically write the connection file of the running endpoint.

        The file is only readable by the owner because it contains the
        endpoint's token.
        """
        data = {
            'host': info.host,
            'port': info.port,
            'token': info.token.hex(),
            'tls_fingerprint': info.tls_fingerprint,
        }
        write_private_file(self.connection_path, json.dumps(data).encode())

    def read_connection(self) -> ConnectionInfo:
        """Read the connection file of the running endpoint.

        Raises:
            FileNotFoundError: If the connection file does not exist (e.g.,
                because the endpoint is not running).
            ValueError: If the connection file is malformed.
        """
        with open(self.connection_path, 'rb') as f:
            contents = f.read()
        try:
            data = json.loads(contents)
            info = ConnectionInfo(
                host=data['host'],
                port=data['port'],
                token=bytes.fromhex(data['token']),
                tls_fingerprint=data['tls_fingerprint'],
            )
        except (TypeError, KeyError, ValueError):
            info = None
        if (
            info is None
            or not isinstance(info.host, str)
            or not isinstance(info.port, int)
            or len(info.token) != TOKEN_SIZE
            or not isinstance(info.tls_fingerprint, (str, type(None)))
        ):
            raise ValueError(
                f'Connection file at {self.connection_path} is malformed.',
            )
        return info

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

    def restrict_permissions(self) -> bool:
        """Remove all group and other permissions from the directory.

        Clients trust the connection file in the endpoint directory, so no
        one other than the owner may be able to create, replace, or rename
        files in it. The directory also contains the endpoint's database and
        log which may contain user data.

        Returns:
            `True` if the permissions of the directory were changed.
        """
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        if mode & 0o077 == 0:
            return False
        os.chmod(self.path, mode & ~0o077)
        return True

    def _join(self, name: str) -> str:
        return os.path.join(self.path, name)


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

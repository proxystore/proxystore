"""Endpoint directory layout and files."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import stat
from typing import Any
from typing import Self

from proxystore.endpoint.auth import ConnectionInfo
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.files import write_private_file
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.peers import PeersConfig
from proxystore.endpoint.peers import read_peers
from proxystore.utils.config import dump
from proxystore.utils.config import dumps
from proxystore.utils.config import load
from proxystore.utils.environment import home_dir


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
            options: Other fields of the
                [`EndpointConfig`][proxystore.endpoint.config.EndpointConfig]
                (e.g., `port`). The `name` and `id` are set automatically.

        Returns:
            The new endpoint directory.

        Raises:
            FileExistsError: If an endpoint with the name already exists.
            ValueError: If the configuration is invalid.
        """
        secret_key = SecretKey.generate() if secret_key is None else secret_key
        config = EndpointConfig(
            name=name,
            id=secret_key.endpoint_id,
            **options,
        )
        endpoint_dir = cls.from_name(name, proxystore_dir)
        os.makedirs(os.path.dirname(endpoint_dir.path), exist_ok=True)
        # Clients trust the files in the endpoint directory, so only the
        # owner can create or replace files in it.
        try:
            os.mkdir(endpoint_dir.path, mode=0o700)
        except FileExistsError:
            raise FileExistsError(
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
    def secret_key_path(self) -> str:
        """Path to the secret key of the endpoint."""
        return self._join('secret.key')

    def write_secret_key(self, secret_key: SecretKey) -> None:
        """Atomically write the secret key of the endpoint.

        The file is only readable by the owner.
        """
        write_private_file(self.secret_key_path, secret_key.to_bytes())

    def read_secret_key(self) -> SecretKey:
        """Read the secret key of the endpoint.

        The key is checked against the ID in the configuration of the
        endpoint, if the configuration exists.

        Raises:
            FileNotFoundError: If the secret key file does not exist.
            ValueError: If the secret key file is malformed or does not
                match the ID in the configuration.
        """
        try:
            with open(self.secret_key_path, 'rb') as f:
                data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f'Endpoint directory {self.path} does not contain a secret '
                'key. Remove the endpoint and configure it again with '
                '"proxystore-endpoint configure".',
            ) from None
        try:
            secret_key = SecretKey(data)
        except ValueError:
            raise ValueError(
                f'Secret key file at {self.secret_key_path} is malformed.',
            ) from None

        if os.path.exists(self.config_path):
            config = self.read_config()
            if secret_key.endpoint_id != config.id:
                raise ValueError(
                    f'The endpoint ID in the configuration ({config.id}) '
                    'does not match the secret key '
                    f'({secret_key.endpoint_id}) in {self.path}.',
                )
        return secret_key

    @property
    def peers_path(self) -> str:
        """Path to the allowlist of peer endpoints."""
        return self._join('peers.toml')

    def read_peers(self) -> PeersConfig:
        """Read the allowlist of peer endpoints.

        Returns:
            The allowlist or an empty allowlist if the file does not exist.

        Raises:
            ValueError: If the allowlist cannot be parsed or is invalid.
        """
        return read_peers(self.peers_path)

    def write_peers(self, peers: PeersConfig) -> None:
        """Atomically write the allowlist of peer endpoints."""
        write_private_file(self.peers_path, dumps(peers).encode())

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

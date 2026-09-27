"""Endpoint directory layout and files.

Warning:
    This module is an internal implementation detail. Its interface may
    change between releases without notice (see
    [`proxystore.endpoint`][proxystore.endpoint]).
"""

from __future__ import annotations

import contextlib
import dataclasses
import enum
import errno
import logging
import os
import random
import shutil
import stat
import sys
from typing import ClassVar
from typing import Self
from typing import TypedDict
from typing import Unpack

from pydantic import ConfigDict

from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.config import EndpointP2PConfig
from proxystore.endpoint.config import EndpointStorageConfig
from proxystore.endpoint.exceptions import EndpointConfigError
from proxystore.endpoint.exceptions import EndpointExistsError
from proxystore.endpoint.exceptions import EndpointNotFoundError
from proxystore.endpoint.exceptions import EndpointRunningError
from proxystore.endpoint.files import read_model
from proxystore.endpoint.files import VersionedFile
from proxystore.endpoint.files import write_model
from proxystore.endpoint.files import write_private_file
from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SecretKey
from proxystore.endpoint.peers import Peers
from proxystore.utils.environment import home_dir
from proxystore.utils.environment import hostname

if sys.platform != 'win32':  # pragma: no branch
    import fcntl

logger = logging.getLogger(__name__)

_LOCK_UNSUPPORTED_ERRNOS = frozenset(
    (errno.ENOLCK, errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL),
)

CONNECTION_VERSION = 1
"""Format version of the connection file."""


class ConnectionInfo(VersionedFile):
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

    model_config = ConfigDict(frozen=True)
    DESCRIPTION: ClassVar[str] = 'connection file'

    version: int = CONNECTION_VERSION
    host: str
    port: int
    token: EndpointToken
    tls_fingerprint: str | None
    hostname: str
    pid: int


class EndpointOptions(TypedDict, total=False):
    """Options of a new endpoint.

    See [`EndpointConfig`][proxystore.endpoint.config.EndpointConfig] for
    the meaning and default of each option.
    """

    host: str
    tls: bool
    max_object_size: int | str
    p2p: EndpointP2PConfig
    storage: EndpointStorageConfig


class EndpointStatus(enum.Enum):
    """Status of an endpoint.

    See
    [`EndpointDir.status()`][proxystore.endpoint.directory.EndpointDir.status].
    """

    RUNNING = enum.auto()
    """Endpoint is running."""
    STOPPED = enum.auto()
    """Endpoint is not running."""
    STALE = enum.auto()
    """Endpoint on this host stopped without removing its connection file.

    This happens if the endpoint process was killed or crashed. Starting or
    stopping the endpoint removes the stale connection file.
    """
    OTHER_HOST = enum.auto()
    """Endpoint was started on another host and may still be running there.

    The connection file was written by an endpoint on another host which
    shares the endpoint directory. Whether the endpoint is still running on
    that host cannot be checked from this host.
    """
    UNKNOWN = enum.auto()
    """Endpoint cannot be found (missing/corrupted directory)."""


class EndpointLock:
    """Advisory lock held by a running endpoint.

    A running endpoint holds an exclusive lock (see
    [`fcntl.flock()`][fcntl.flock]) on the lock file in its directory for as
    long as it runs. The operating system releases the lock when the process
    exits, even if the process crashes, so the lock reliably shows if the
    endpoint is running on this host. Whether the lock is also visible to
    other hosts depends on the file system of the endpoint directory.

    Some file systems (e.g., some network or parallel file systems) do not
    support locks. Then, acquiring the lock always succeeds, `supported` is
    `False`, and the status of the endpoint is determined from the PID in
    its connection file instead.

    Attributes:
        path: Path of the lock file.
        supported: If the file system supports locks. This is `True` until
            locking fails because locks are not supported.

    Args:
        path: Path of the lock file.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self.supported = sys.platform != 'win32'
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        """This lock object holds the lock."""
        return self._fd is not None

    def acquire(self) -> None:
        """Acquire the lock without waiting.

        Raises:
            RuntimeError: If this lock object already holds the lock.
            EndpointRunningError: If another process (or another lock object
                in this process) holds the lock.
        """
        if self._fd is not None:
            raise RuntimeError('The lock is already held.')
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if self._flock(fd) is False:
                raise EndpointRunningError(
                    'Endpoint '
                    f'{os.path.basename(os.path.dirname(self.path))} is '
                    f'already running (its lock {self.path} is held by '
                    'another process).',
                )
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        """Release the lock if it is held."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        if self.supported:  # pragma: no branch
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def is_locked(self) -> bool | None:
        """Check if the lock is held.

        Returns:
            `True` if the lock is held by any lock object, `False` if the \
            lock is not held, or `None` if locks are not supported.
        """
        try:
            fd = os.open(self.path, os.O_RDWR)
        except FileNotFoundError:
            return False if self.supported else None
        try:
            acquired = self._flock(fd)
            if acquired is None:
                return None
            if acquired:
                fcntl.flock(fd, fcntl.LOCK_UN)
            return not acquired
        finally:
            os.close(fd)

    def _flock(self, fd: int) -> bool | None:
        # Returns if the lock was acquired or None if locks are unsupported.
        if not self.supported:  # pragma: no cover
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        except OSError as e:
            if e.errno not in _LOCK_UNSUPPORTED_ERRNOS:
                raise
            logger.warning(
                'The file system of %s does not support locks so the status '
                'of the endpoint is determined from its PID: %s',
                self.path,
                e,
            )
            self.supported = False
            return None
        return True


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

    @property
    def name(self) -> str:
        """Name of the endpoint (i.e., the name of the directory)."""
        return os.path.basename(os.path.normpath(self.path))

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
        if proxystore_dir is None:
            proxystore_dir = home_dir()
        return cls(os.path.join(proxystore_dir, name))

    @classmethod
    def create(
        cls,
        name: str,
        proxystore_dir: str | None = None,
        *,
        secret_key: SecretKey | None = None,
        port: int | None = None,
        **options: Unpack[EndpointOptions],
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
            options: Other options of the endpoint (see
                [`EndpointOptions`][proxystore.endpoint.directory.EndpointOptions]).

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
        if proxystore_dir is None:
            proxystore_dir = home_dir()
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
            config = read_model(EndpointConfig, self.config_path)
        except FileNotFoundError:
            self.check_exists()
            raise EndpointNotFoundError(
                f'Endpoint directory {self.path} does not contain a valid '
                'configuration.',
            ) from None

        # The directory name is used to find an endpoint by name, so the
        # name in the configuration must match it.
        if config.name != self.name:
            raise EndpointConfigError(
                f'The endpoint name in {self.config_path} ({config.name}) '
                f'does not match the name of the directory ({self.name}). '
                'Rename the directory or change the name in the '
                'configuration so they match.',
            )
        return config

    def write_config(self, config: EndpointConfig) -> None:
        """Atomically write the endpoint configuration.

        The directory is created, only accessible by the owner, if needed.

        Args:
            config: Configuration to write.
        """
        os.makedirs(self.path, mode=0o700, exist_ok=True)
        write_model(self.config_path, config)

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
    def lock_path(self) -> str:
        """Path to the lock file held by the running endpoint."""
        return self._join('endpoint.lock')

    def lock(self) -> EndpointLock:
        """Get the lock held by the running endpoint.

        See [`EndpointLock`][proxystore.endpoint.directory.EndpointLock].
        """
        return EndpointLock(self.lock_path)

    @property
    def secret_key_path(self) -> str:
        """Path to the secret key of the endpoint."""
        return self._join('secret.key')

    def write_secret_key(self, secret_key: SecretKey) -> None:
        """Atomically write the secret key of the endpoint.

        The file is only readable by the owner.
        """
        write_private_file(self.secret_key_path, secret_key.to_bytes())

    def read_secret_key(self, endpoint_id: EndpointId) -> SecretKey:
        """Read the secret key of the endpoint.

        Args:
            endpoint_id: ID the secret key must match (i.e., the ID in the
                configuration of the endpoint).

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

        if secret_key.endpoint_id != endpoint_id:
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

    def peers(self) -> Peers:
        """Get the peers of the endpoint.

        This reads the configuration to get the ID of the endpoint.

        Raises:
            EndpointNotFoundError: If the configuration does not exist.
            EndpointConfigError: If the configuration is invalid.
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
        write_model(self.connection_path, info)

    def read_connection(self) -> ConnectionInfo:
        """Read the connection file of the running endpoint.

        Raises:
            FileNotFoundError: If the connection file does not exist (e.g.,
                because the endpoint is not running).
            EndpointConfigError: If the connection file is malformed or has
                an unsupported format version.
        """
        return read_model(ConnectionInfo, self.connection_path)

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

    def check_not_running_elsewhere(self) -> None:
        """Check that the endpoint is not running on a different host.

        The connection file records the host the endpoint was started on.

        Raises:
            EndpointRunningError: If the connection file was written by an
                endpoint on another host.
        """
        try:
            info = self.read_connection()
        except (OSError, EndpointConfigError):
            # The file does not exist or cannot be read so it is never used.
            return
        if info.hostname == hostname():
            return
        raise EndpointRunningError(
            f'Endpoint {self.name} appears to be running on {info.hostname} '
            f'(PID {info.pid}). Stop the endpoint on {info.hostname}. If it '
            'is not running, delete the connection file at '
            f'{self.connection_path} and try again.',
        )

    def status(self) -> EndpointStatus:
        """Get the status of the endpoint.

        The endpoint is running if it holds its lock (see
        [`lock()`][proxystore.endpoint.directory.EndpointDir.lock]). If the
        file system does not support locks, the endpoint is running if the
        process in its connection file is running on this host.

        Returns:
            The status of the endpoint.
        """
        if not os.path.isdir(self.path):
            return EndpointStatus.UNKNOWN
        try:
            self.read_config()
        except (FileNotFoundError, ValueError) as e:
            logger.debug('Unable to read endpoint configuration: %s', e)
            return EndpointStatus.UNKNOWN

        locked = self.lock().is_locked()
        if locked:
            return EndpointStatus.RUNNING
        try:
            info = self.read_connection()
        except FileNotFoundError:
            return EndpointStatus.STOPPED
        except (OSError, EndpointConfigError):
            # A connection file which cannot be read is never used.
            return EndpointStatus.STALE
        if info.hostname != hostname():
            return EndpointStatus.OTHER_HOST
        if locked is None and is_own_process(info.pid):
            return EndpointStatus.RUNNING
        return EndpointStatus.STALE

    def remove(self) -> None:
        """Remove the endpoint directory and all of its files.

        Raises:
            EndpointNotFoundError: If the endpoint directory does not exist.
            EndpointRunningError: If the endpoint is running or may be
                running on another host.
        """
        self.check_exists()
        status = self.status()
        if status == EndpointStatus.OTHER_HOST:
            self.check_not_running_elsewhere()
        if status == EndpointStatus.RUNNING:
            raise EndpointRunningError(
                f'Endpoint {self.name} must be stopped before it is removed.',
            )
        shutil.rmtree(self.path)

    def check_exists(self) -> None:
        """Check that the endpoint directory exists.

        Raises:
            EndpointNotFoundError: If the endpoint directory does not exist.
        """
        if not os.path.isdir(self.path):
            raise EndpointNotFoundError(
                f'An endpoint named {self.name} does not exist in '
                f'{os.path.dirname(os.path.normpath(self.path))}.',
            )

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

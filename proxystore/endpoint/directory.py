"""Endpoint directory layout and files."""

from __future__ import annotations

import contextlib
import dataclasses
import os
import ssl
import stat

from proxystore.endpoint.auth import Credentials
from proxystore.endpoint.auth import generate_tls_certificate
from proxystore.endpoint.auth import generate_token_file
from proxystore.endpoint.auth import read_certificate_fingerprint
from proxystore.endpoint.auth import read_token_file


@dataclasses.dataclass(frozen=True)
class EndpointDir:
    """Directory of an endpoint.

    An endpoint directory contains the endpoint's configuration and the
    files created while it runs (e.g., its log and the credentials that
    clients use to connect).

    Example:
        ```python
        endpoint_dir = EndpointDir('/path/to/endpoint')
        assert endpoint_dir.config_path == '/path/to/endpoint/config.toml'
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
    def token_path(self) -> str:
        """Path to the token clients use to authenticate."""
        return self._join('client.token')

    @property
    def tls_cert_path(self) -> str:
        """Path to the TLS certificate of the endpoint."""
        return self._join('tls.crt')

    @property
    def tls_key_path(self) -> str:
        """Path to the TLS private key of the endpoint."""
        return self._join('tls.key')

    def create_credentials(
        self, *, tls: bool, common_name: str
    ) -> Credentials:
        """Create new credentials for clients.

        This writes a new token and, if `tls` is set, a new self-signed TLS
        certificate and private key, replacing any existing files.

        Args:
            tls: Generate a TLS certificate.
            common_name: Common name of the TLS certificate subject.
        """
        token = generate_token_file(self.token_path)
        fingerprint = None
        if tls:
            generate_tls_certificate(
                self.tls_cert_path,
                self.tls_key_path,
                common_name,
            )
            fingerprint = read_certificate_fingerprint(self.tls_cert_path)
        return Credentials(token, fingerprint)

    def load_credentials(self, *, tls: bool) -> Credentials:
        """Load the credentials of the running endpoint.

        Args:
            tls: Load the fingerprint of the endpoint's TLS certificate.

        Raises:
            FileNotFoundError: If the token or certificate file does not
                exist (e.g., because the endpoint is not running).
            ValueError: If the token file is malformed.
        """
        token = read_token_file(self.token_path)
        fingerprint = (
            read_certificate_fingerprint(self.tls_cert_path) if tls else None
        )
        return Credentials(token, fingerprint)

    def remove_credentials(self) -> None:
        """Remove the credential files, ignoring any that do not exist."""
        for path in (self.token_path, self.tls_cert_path, self.tls_key_path):
            with contextlib.suppress(FileNotFoundError):
                os.remove(path)

    def server_ssl_context(self) -> ssl.SSLContext:
        """Create an SSL context with the TLS certificate of the endpoint.

        The certificate must have been created by
        [`create_credentials()`][proxystore.endpoint.directory.EndpointDir.create_credentials].
        """
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(self.tls_cert_path, self.tls_key_path)
        return context

    def restrict_permissions(self) -> bool:
        """Remove group and other write permissions from the directory.

        Clients trust the token and TLS certificate in the endpoint
        directory, so no one other than the owner may be able to create,
        replace, or rename files in it.

        Returns:
            `True` if the permissions of the directory were changed.
        """
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        if mode & 0o022 == 0:
            return False
        os.chmod(self.path, mode & ~0o022)
        return True

    def _join(self, name: str) -> str:
        return os.path.join(self.path, name)

"""Authentication between clients and their local endpoint.

Each time an endpoint starts, it generates a random token and writes it,
along with its address, to a connection file in the endpoint's directory
that only the owner can read (see
[`ConnectionInfo`][proxystore.endpoint.auth.ConnectionInfo]). Clients read
the connection file, so any process that can read the user's ProxyStore
home directory is trusted.

The token is never sent over the network. Instead, the client and endpoint
each prove they know the token by computing an HMAC over random nonces
chosen by both sides. This authenticates the client to the endpoint and the
endpoint to the client (i.e., a different server listening on the endpoint's
address cannot impersonate the endpoint).

Optionally, connections can be encrypted with TLS. The endpoint generates a
new self-signed certificate each time it starts, and clients only trust the
certificate whose fingerprint is in the connection file (i.e., certificate
pinning).
"""

from __future__ import annotations

import contextlib
import datetime
import hashlib
import hmac
import os
import secrets
import ssl
import tempfile
from typing import Literal
from typing import NamedTuple

from proxystore.serialize import BytesLike

TOKEN_SIZE = 32
"""Size in bytes of an endpoint token."""


class ConnectionInfo(NamedTuple):
    """Information that clients use to connect to a running endpoint.

    The endpoint writes this to its directory each time it starts and
    removes it when it stops (see
    [`EndpointDir`][proxystore.endpoint.directory.EndpointDir]).

    Attributes:
        host: Host address the endpoint is listening on.
        port: Port the endpoint is listening on.
        token: Token that the client and endpoint prove they know.
        tls_fingerprint: SHA-256 fingerprint of the endpoint's TLS
            certificate or `None` if the endpoint does not use TLS.
    """

    host: str
    port: int
    token: bytes
    tls_fingerprint: str | None


def write_private_file(path: str, data: BytesLike) -> None:
    """Atomically write data to a file that only the owner can access.

    The data is written to a temporary file with mode `0600` in the same
    directory which then replaces `path`, so readers never observe a
    partially written file.
    """
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(path) or '.',
        prefix=f'.{os.path.basename(path)}.',
    )
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)
        raise


def generate_token() -> bytes:
    """Generate a new random endpoint token."""
    return secrets.token_bytes(TOKEN_SIZE)


def compute_proof(
    token: bytes,
    role: Literal['client', 'server'],
    first_nonce: bytes,
    second_nonce: bytes,
) -> bytes:
    """Compute a proof that the sender knows the token.

    The role is included so a proof sent by one side can never be replayed
    as the proof of the other side.

    Args:
        token: Endpoint token.
        role: Role of the side computing the proof.
        first_nonce: Nonce of the side computing the proof.
        second_nonce: Nonce of the other side.
    """
    message = role.encode() + first_nonce + second_nonce
    return hmac.new(token, message, hashlib.sha256).digest()


def verify_proof(
    token: bytes,
    role: Literal['client', 'server'],
    first_nonce: bytes,
    second_nonce: bytes,
    proof: bytes,
) -> bool:
    """Verify a proof computed by the other side of the handshake."""
    expected = compute_proof(token, role, first_nonce, second_nonce)
    return hmac.compare_digest(expected, proof)


def generate_tls_certificate(common_name: str) -> tuple[bytes, bytes]:
    """Generate a self-signed TLS certificate and private key.

    Note:
        This requires the `cryptography` package which is included in the
        `endpoints` extra.

    Args:
        common_name: Common name of the certificate subject.

    Returns:
        Tuple of the PEM-encoded certificate and private key.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return cert_pem, key_pem


def server_ssl_context(cert_pem: bytes, key_pem: bytes) -> ssl.SSLContext:
    """Create a server SSL context from a PEM-encoded certificate and key.

    The certificate and key are never written to the endpoint directory.
    [`SSLContext.load_cert_chain()`][ssl.SSLContext.load_cert_chain] only
    accepts file paths, so they are briefly written to a private temporary
    directory.
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    with tempfile.TemporaryDirectory() as tmp_dir:
        cert_path = os.path.join(tmp_dir, 'tls.crt')
        key_path = os.path.join(tmp_dir, 'tls.key')
        write_private_file(cert_path, cert_pem)
        write_private_file(key_path, key_pem)
        context.load_cert_chain(cert_path, key_path)
    return context


def certificate_fingerprint(der: BytesLike) -> str:
    """Compute the SHA-256 fingerprint of a DER-encoded certificate."""
    return hashlib.sha256(der).hexdigest()


def pem_certificate_fingerprint(cert_pem: bytes) -> str:
    """Compute the SHA-256 fingerprint of a PEM-encoded certificate."""
    return certificate_fingerprint(ssl.PEM_cert_to_DER_cert(cert_pem.decode()))

"""Authentication between clients and their local endpoint.

Each time an endpoint starts, it generates a random token and writes it to
a file in the endpoint's directory that only the owner can read. Clients
read the token from the same directory, so any process that can read the
user's ProxyStore home directory is trusted.

The token is never sent over the network. Instead, the client and endpoint
each prove they know the token by computing an HMAC over random nonces
chosen by both sides. This authenticates the client to the endpoint and the
endpoint to the client (i.e., a different server listening on the endpoint's
address cannot impersonate the endpoint).

Optionally, connections can be encrypted with TLS. The endpoint generates a
new self-signed certificate each time it starts, and clients only trust the
certificate in the endpoint's directory (i.e., certificate pinning).
"""

from __future__ import annotations

import datetime
import hashlib
import hmac
import os
import secrets
import ssl
from typing import Literal

TOKEN_SIZE = 32
"""Size in bytes of an endpoint token."""


def write_private_file(path: str, data: bytes) -> None:
    """Write data to a file that only the owner can read and write.

    The file is created with mode `0600`. If the file already exists, it is
    truncated and its mode is reset to `0600`.
    """
    _write_file(path, data, 0o600)


def _write_file(path: str, data: bytes, mode: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        # The mode passed to open() only applies when the file is created
        # and is masked by the umask (e.g., the endpoint daemon's umask
        # makes new files group-writable), so always set the mode.
        os.fchmod(fd, mode)
        os.write(fd, data)
    finally:
        os.close(fd)


def generate_token_file(path: str) -> bytes:
    """Generate a new random token and write it to a file.

    Args:
        path: Path of the token file.

    Returns:
        The token.
    """
    token = secrets.token_bytes(TOKEN_SIZE)
    write_private_file(path, token.hex().encode())
    return token


def read_token_file(path: str) -> bytes:
    """Read a token from a file.

    Raises:
        FileNotFoundError: If the token file does not exist.
        ValueError: If the file does not contain a valid token.
    """
    with open(path) as f:
        contents = f.read().strip()
    try:
        token = bytes.fromhex(contents)
    except ValueError:
        raise ValueError(f'Token file at {path} is malformed.') from None
    if len(token) != TOKEN_SIZE:
        raise ValueError(f'Token file at {path} is malformed.')
    return token


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


def generate_tls_certificate(
    cert_path: str,
    key_path: str,
    common_name: str,
) -> None:
    """Generate a self-signed TLS certificate and private key.

    Note:
        This requires the `cryptography` package which is included in the
        `endpoints` extra.

    Args:
        cert_path: Path to write the PEM-encoded certificate to.
        key_path: Path to write the PEM-encoded private key to. The file
            is only readable by the owner.
        common_name: Common name of the certificate subject.
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

    write_private_file(
        key_path,
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
    )
    # Clients trust whatever certificate is in this file, so only the owner
    # can modify it.
    _write_file(
        cert_path, cert.public_bytes(serialization.Encoding.PEM), 0o644
    )


def certificate_fingerprint(der: bytes) -> str:
    """Compute the SHA-256 fingerprint of a DER-encoded certificate."""
    return hashlib.sha256(der).hexdigest()


def read_certificate_fingerprint(cert_path: str) -> str:
    """Read a PEM-encoded certificate and compute its fingerprint.

    Raises:
        FileNotFoundError: If the certificate file does not exist.
    """
    with open(cert_path) as f:
        der = ssl.PEM_cert_to_DER_cert(f.read())
    return certificate_fingerprint(der)

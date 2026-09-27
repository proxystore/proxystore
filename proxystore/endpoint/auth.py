"""Authentication between clients and their local endpoint.

Each time an endpoint starts, it generates a random token and writes it,
along with its address, to a connection file in the endpoint's directory
that only the owner can read (see
[`ConnectionInfo`][proxystore.endpoint.directory.ConnectionInfo]). Clients read
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

import dataclasses
import datetime
import hashlib
import hmac
import os
import secrets
import ssl
import tempfile
from typing import Any
from typing import Literal
from typing import Self

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from pydantic_core import CoreSchema

from proxystore.endpoint.files import write_private_file
from proxystore.serialize import BytesLike


class EndpointToken:
    """Token that a client and endpoint prove they know.

    The token is never included in its `repr()` so it is not accidentally
    logged, and tokens are compared in constant time. In pydantic models,
    the token is validated from and serialized to its hex encoding.

    Args:
        token: Token as 32 bytes.

    Raises:
        ValueError: If `token` is not the correct size.
    """

    __slots__ = ('_token',)
    _SIZE = 32

    def __init__(self, token: bytes) -> None:
        if not isinstance(token, bytes) or len(token) != self._SIZE:
            raise ValueError(f'Endpoint token must be {self._SIZE} bytes.')
        self._token = token

    def __repr__(self) -> str:
        return f'{type(self).__name__}(<redacted>)'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EndpointToken):
            return NotImplemented
        return hmac.compare_digest(self._token, other._token)

    def __hash__(self) -> int:
        return hash(self._token)

    @classmethod
    def generate(cls) -> Self:
        """Generate a new random token."""
        return cls(secrets.token_bytes(cls._SIZE))

    @classmethod
    def from_hex(cls, value: str) -> Self:
        """Decode a hex-encoded token.

        Raises:
            ValueError: If `value` is not a valid hex-encoded token.
        """
        return cls(bytes.fromhex(value))

    def hex(self) -> str:
        """Encode the token as hex."""
        return self._token.hex()

    @classmethod
    def _validate(cls, value: Any) -> Self:
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError('Endpoint token must be a hex-encoded string.')
        return cls.from_hex(value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda token: token.hex(),
                when_used='always',
            ),
        )

    def proof(
        self,
        role: Literal['client', 'server'],
        first_nonce: bytes,
        second_nonce: bytes,
    ) -> bytes:
        """Compute a proof that the sender knows the token.

        The role is included so a proof sent by one side can never be
        replayed as the proof of the other side.

        Args:
            role: Role of the side computing the proof.
            first_nonce: Nonce of the side computing the proof.
            second_nonce: Nonce of the other side.
        """
        message = role.encode() + first_nonce + second_nonce
        return hmac.new(self._token, message, hashlib.sha256).digest()

    def verify(
        self,
        role: Literal['client', 'server'],
        first_nonce: bytes,
        second_nonce: bytes,
        proof: bytes,
    ) -> bool:
        """Verify a proof computed by the other side of the handshake.

        Args:
            role: Role of the side that computed the proof.
            first_nonce: Nonce of the side that computed the proof.
            second_nonce: Nonce of the other side.
            proof: Proof to verify.
        """
        expected = self.proof(role, first_nonce, second_nonce)
        return hmac.compare_digest(expected, proof)


@dataclasses.dataclass(frozen=True, repr=False)
class TLSCertificate:
    """Self-signed TLS certificate and private key of an endpoint.

    The private key is never included in its `repr()`.

    Attributes:
        cert_pem: PEM-encoded certificate.
        key_pem: PEM-encoded private key.
    """

    cert_pem: bytes
    key_pem: bytes

    def __repr__(self) -> str:
        return f'{type(self).__name__}(fingerprint={self.fingerprint!r})'

    @classmethod
    def generate(cls, common_name: str) -> Self:
        """Generate a self-signed TLS certificate and private key.

        Args:
            common_name: Common name of the certificate subject.
        """
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, common_name)],
        )
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
        return cls(
            cert_pem=cert.public_bytes(serialization.Encoding.PEM),
            key_pem=key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            ),
        )

    @property
    def fingerprint(self) -> str:
        """SHA-256 fingerprint of the certificate."""
        der = ssl.PEM_cert_to_DER_cert(self.cert_pem.decode())
        return certificate_fingerprint(der)

    def ssl_context(self) -> ssl.SSLContext:
        """Create a server SSL context with the certificate and key.

        The certificate and key are never written to the endpoint directory.
        [`SSLContext.load_cert_chain()`][ssl.SSLContext.load_cert_chain]
        only accepts file paths, so they are briefly written to a private
        temporary directory.
        """
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        with tempfile.TemporaryDirectory() as tmp_dir:
            cert_path = os.path.join(tmp_dir, 'tls.crt')
            key_path = os.path.join(tmp_dir, 'tls.key')
            write_private_file(cert_path, self.cert_pem)
            write_private_file(key_path, self.key_pem)
            context.load_cert_chain(cert_path, key_path)
        return context


def certificate_fingerprint(der: BytesLike) -> str:
    """Compute the SHA-256 fingerprint of a DER-encoded certificate."""
    return hashlib.sha256(der).hexdigest()

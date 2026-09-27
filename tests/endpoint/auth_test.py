from __future__ import annotations

import os
import pathlib
import ssl
import stat
from typing import Any
from unittest import mock

import pytest

from proxystore.endpoint.auth import certificate_fingerprint
from proxystore.endpoint.auth import EndpointToken
from proxystore.endpoint.auth import TLSCertificate
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.files import write_private_file


def _mode(path: pathlib.Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def test_write_private_file_mode(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'file'
    old_umask = os.umask(0)
    try:
        write_private_file(str(path), b'secret')
    finally:
        os.umask(old_umask)
    assert _mode(path) == 0o600
    assert path.read_bytes() == b'secret'


def test_write_private_file_resets_existing_mode(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / 'file'
    path.write_bytes(b'old contents that are longer')
    os.chmod(path, 0o644)

    write_private_file(str(path), b'new')

    assert _mode(path) == 0o600
    assert path.read_bytes() == b'new'


def test_write_private_file_is_atomic(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'file'
    path.write_bytes(b'old')
    with (
        mock.patch('os.replace', side_effect=OSError('failed')),
        pytest.raises(OSError, match='failed'),
    ):
        write_private_file(str(path), b'new')

    # The original file is untouched and the temporary file is removed
    assert path.read_bytes() == b'old'
    assert os.listdir(tmp_path) == ['file']


def test_token() -> None:
    token = EndpointToken.generate()
    assert EndpointToken.generate() != token
    assert EndpointToken.from_hex(token.hex()) == token
    assert hash(EndpointToken.from_hex(token.hex())) == hash(token)
    assert token != token.hex()
    # The token is never in the repr
    assert repr(token) == 'EndpointToken(<redacted>)'
    assert token.hex() not in repr(token)


@pytest.mark.parametrize('value', (b'short', 'a' * TOKEN_SIZE, b'x' * 33))
def test_token_invalid(value: Any) -> None:
    with pytest.raises(ValueError, match='must be 32 bytes'):
        EndpointToken(value)


def test_token_from_hex_invalid() -> None:
    with pytest.raises(ValueError, match='non-hexadecimal'):
        EndpointToken.from_hex('not hex')


def test_proof_verification() -> None:
    token = EndpointToken.generate()
    client_nonce, server_nonce = os.urandom(32), os.urandom(32)

    proof = token.proof('server', server_nonce, client_nonce)
    assert token.verify('server', server_nonce, client_nonce, proof)

    # Wrong token
    other_token = EndpointToken.generate()
    assert not other_token.verify('server', server_nonce, client_nonce, proof)
    # A server proof cannot be used as a client proof
    assert not token.verify('client', server_nonce, client_nonce, proof)
    # Nonces are not interchangeable
    assert not token.verify('server', client_nonce, server_nonce, proof)


def test_tls_certificate() -> None:
    certificate = TLSCertificate.generate('test')

    # Certificate and key are a valid pair
    context = certificate.ssl_context()
    assert isinstance(context, ssl.SSLContext)

    der = ssl.PEM_cert_to_DER_cert(certificate.cert_pem.decode())
    assert certificate.fingerprint == certificate_fingerprint(der)

    # The private key is never in the repr
    assert repr(certificate) == (
        f'TLSCertificate(fingerprint={certificate.fingerprint!r})'
    )
    assert certificate.key_pem.decode() not in repr(certificate)

    # A new certificate is generated each time
    assert TLSCertificate.generate('test').fingerprint != (
        certificate.fingerprint
    )

from __future__ import annotations

import os
import pathlib
import ssl
import stat
from unittest import mock

import pytest

from proxystore.endpoint.auth import certificate_fingerprint
from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import generate_tls_certificate
from proxystore.endpoint.auth import generate_token
from proxystore.endpoint.auth import pem_certificate_fingerprint
from proxystore.endpoint.auth import server_ssl_context
from proxystore.endpoint.auth import TOKEN_SIZE
from proxystore.endpoint.auth import verify_proof
from proxystore.endpoint.auth import write_private_file


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
    with mock.patch('os.replace', side_effect=OSError('failed')):
        with pytest.raises(OSError, match='failed'):
            write_private_file(str(path), b'new')

    # The original file is untouched and the temporary file is removed
    assert path.read_bytes() == b'old'
    assert os.listdir(tmp_path) == ['file']


def test_generate_token() -> None:
    token = generate_token()
    assert len(token) == TOKEN_SIZE
    assert generate_token() != token


def test_proof_verification() -> None:
    token = os.urandom(TOKEN_SIZE)
    client_nonce, server_nonce = os.urandom(32), os.urandom(32)

    proof = compute_proof(token, 'server', server_nonce, client_nonce)
    assert verify_proof(token, 'server', server_nonce, client_nonce, proof)

    # Wrong token
    other_token = os.urandom(TOKEN_SIZE)
    assert not verify_proof(
        other_token,
        'server',
        server_nonce,
        client_nonce,
        proof,
    )
    # A server proof cannot be used as a client proof
    assert not verify_proof(token, 'client', server_nonce, client_nonce, proof)
    # Nonces are not interchangeable
    assert not verify_proof(token, 'server', client_nonce, server_nonce, proof)


def test_generate_tls_certificate() -> None:
    cert_pem, key_pem = generate_tls_certificate('test')

    # Certificate and key are a valid pair
    context = server_ssl_context(cert_pem, key_pem)
    assert isinstance(context, ssl.SSLContext)

    der = ssl.PEM_cert_to_DER_cert(cert_pem.decode())
    fingerprint = pem_certificate_fingerprint(cert_pem)
    assert fingerprint == certificate_fingerprint(der)

    # A new certificate is generated each time
    new_cert_pem, _ = generate_tls_certificate('test')
    assert pem_certificate_fingerprint(new_cert_pem) != fingerprint

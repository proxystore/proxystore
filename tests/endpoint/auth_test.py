from __future__ import annotations

import os
import pathlib
import ssl
import stat

import pytest

from proxystore.endpoint.auth import certificate_fingerprint
from proxystore.endpoint.auth import compute_proof
from proxystore.endpoint.auth import generate_tls_certificate
from proxystore.endpoint.auth import generate_token_file
from proxystore.endpoint.auth import read_certificate_fingerprint
from proxystore.endpoint.auth import read_token_file
from proxystore.endpoint.auth import restrict_directory
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


@pytest.mark.parametrize(
    ('mode', 'expected'),
    ((0o700, 0o700), (0o755, 0o755), (0o775, 0o755), (0o777, 0o755)),
)
def test_restrict_directory(
    mode: int,
    expected: int,
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / 'dir'
    path.mkdir()
    os.chmod(path, mode)
    assert restrict_directory(str(path)) == (mode != expected)
    assert _mode(path) == expected


def test_token_file_round_trip(tmp_path: pathlib.Path) -> None:
    path = str(tmp_path / 'token')
    token = generate_token_file(path)
    assert len(token) == TOKEN_SIZE
    assert read_token_file(path) == token
    assert _mode(tmp_path / 'token') == 0o600

    # A new token is generated each time
    assert generate_token_file(path) != token


@pytest.mark.parametrize('contents', ('not hex', 'abcd'))
def test_read_token_file_malformed(
    contents: str,
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / 'token'
    path.write_text(contents)
    with pytest.raises(ValueError, match='malformed'):
        read_token_file(str(path))


def test_read_token_file_missing(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_token_file(str(tmp_path / 'token'))


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


def test_generate_tls_certificate(tmp_path: pathlib.Path) -> None:
    cert_path, key_path = tmp_path / 'tls.crt', tmp_path / 'tls.key'
    # Permissive umask like the endpoint daemon uses
    old_umask = os.umask(0o002)
    try:
        generate_tls_certificate(str(cert_path), str(key_path), 'test')
    finally:
        os.umask(old_umask)
    assert _mode(key_path) == 0o600
    assert _mode(cert_path) == 0o644

    # Certificate and key are a valid pair
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(str(cert_path), str(key_path))

    der = ssl.PEM_cert_to_DER_cert(cert_path.read_text())
    fingerprint = read_certificate_fingerprint(str(cert_path))
    assert fingerprint == certificate_fingerprint(der)

    # A new certificate is generated each time
    generate_tls_certificate(str(cert_path), str(key_path), 'test')
    assert read_certificate_fingerprint(str(cert_path)) != fingerprint

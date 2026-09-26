from __future__ import annotations

from typing import Any

import pytest

from proxystore.endpoint.identity import endpoint_id_from_secret_key
from proxystore.endpoint.identity import generate_secret_key
from proxystore.endpoint.identity import log_name
from proxystore.endpoint.identity import parse_endpoint_id
from proxystore.endpoint.identity import SECRET_KEY_SIZE
from proxystore.endpoint.identity import short_id
from proxystore.endpoint.identity import validate_public_key

_ID = 'ab' * 32


def test_parse_endpoint_id() -> None:
    assert parse_endpoint_id(_ID) == _ID
    assert parse_endpoint_id(f' {_ID.upper()}\n') == _ID


@pytest.mark.parametrize(
    'value',
    ('', 'ab' * 31, 'ab' * 33, 'zz' * 32, f'{_ID[:-1]}-', None, 42),
)
def test_parse_endpoint_id_invalid(value: Any) -> None:
    with pytest.raises(ValueError, match='not a valid endpoint ID'):
        parse_endpoint_id(value)


def test_short_id_and_log_name() -> None:
    endpoint_id = parse_endpoint_id(_ID)
    assert short_id(endpoint_id) == _ID[:10]
    assert log_name(endpoint_id, 'name') == f'name({_ID[:10]})'


def test_endpoint_id_from_secret_key() -> None:
    secret_key = generate_secret_key()
    assert len(secret_key) == SECRET_KEY_SIZE
    endpoint_id = endpoint_id_from_secret_key(secret_key)
    assert parse_endpoint_id(endpoint_id) == endpoint_id
    # The ID is deterministic for a secret key
    assert endpoint_id_from_secret_key(secret_key) == endpoint_id
    assert endpoint_id_from_secret_key(generate_secret_key()) != endpoint_id


def test_endpoint_id_from_secret_key_bad_size() -> None:
    with pytest.raises(ValueError, match='must be 32 bytes'):
        endpoint_id_from_secret_key(b'abc')


def test_validate_public_key() -> None:
    validate_public_key(endpoint_id_from_secret_key(generate_secret_key()))
    # Well-formed but not a valid ed25519 public key
    with pytest.raises(ValueError, match='not a valid public key'):
        validate_public_key(parse_endpoint_id('02' * 32))

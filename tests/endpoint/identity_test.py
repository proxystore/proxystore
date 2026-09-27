from __future__ import annotations

import json
import os
import pickle
from typing import Any

import iroh
import pydantic
import pytest

from proxystore.endpoint.identity import EndpointId
from proxystore.endpoint.identity import SECRET_KEY_SIZE
from proxystore.endpoint.identity import SecretKey

# Well-formed and a valid public key
_ID = 'aa' * 32


def test_endpoint_id() -> None:
    endpoint_id = EndpointId(_ID)
    assert endpoint_id == _ID
    assert isinstance(endpoint_id, str)
    assert str(endpoint_id) == _ID
    assert repr(endpoint_id) == f"EndpointId('{_ID}')"
    lookup: dict[str, int] = {endpoint_id: 1}
    assert lookup[_ID] == 1
    assert json.dumps(endpoint_id) == f'"{_ID}"'


def test_endpoint_id_constructor_is_strict() -> None:
    # Only from_str() normalizes the value
    with pytest.raises(ValueError, match='not a valid endpoint ID'):
        EndpointId(_ID.upper())


def test_from_str() -> None:
    assert EndpointId.from_str(_ID) == _ID
    assert EndpointId.from_str(f' {_ID.upper()}\n') == _ID
    assert isinstance(EndpointId.from_str(_ID), EndpointId)
    endpoint_id = EndpointId(_ID)
    assert EndpointId.from_str(endpoint_id) is endpoint_id


@pytest.mark.parametrize(
    'value',
    ('', 'ab' * 31, 'ab' * 33, 'zz' * 32, f'{_ID[:-1]}-', None, 42),
)
def test_from_str_invalid(value: Any) -> None:
    with pytest.raises(ValueError, match='not a valid endpoint ID'):
        EndpointId.from_str(value)


def test_short_and_log_name() -> None:
    endpoint_id = EndpointId(_ID)
    assert endpoint_id.short() == _ID[:10]
    assert endpoint_id.log_name('name') == f'name({_ID[:10]})'


def test_secret_key() -> None:
    secret_key = SecretKey.generate()
    assert len(secret_key.to_bytes()) == SECRET_KEY_SIZE
    endpoint_id = secret_key.endpoint_id
    assert isinstance(endpoint_id, EndpointId)
    # The ID is deterministic for a secret key
    same = SecretKey(secret_key.to_bytes())
    assert same == secret_key
    assert hash(same) == hash(secret_key)
    assert same.endpoint_id == endpoint_id
    other = SecretKey.generate()
    assert other != secret_key
    assert other.endpoint_id != endpoint_id
    assert secret_key != secret_key.to_bytes()


def test_secret_key_repr_hides_key() -> None:
    secret_key = SecretKey.generate()
    text = repr(secret_key)
    assert text == f'SecretKey(endpoint_id={secret_key.endpoint_id!r})'
    assert secret_key.to_bytes().hex() not in text
    assert str(secret_key) == text


def test_secret_key_bad_size() -> None:
    with pytest.raises(ValueError, match='must be 32 bytes'):
        SecretKey(b'abc')


def test_random() -> None:
    endpoint_id = EndpointId.random()
    assert isinstance(endpoint_id, EndpointId)
    assert EndpointId.random() != endpoint_id


_P = 2**255 - 19


@pytest.mark.parametrize(
    'key',
    (
        bytes(32),
        b'\x01' + bytes(31),
        # x = 0 with the sign bit set
        b'\x01' + bytes(30) + b'\x80',
        bytes(31) + b'\x80',
        # Non-canonical encodings of y (y >= p)
        (_P).to_bytes(32, 'little'),
        (_P + 1).to_bytes(32, 'little'),
        b'\xff' * 32,
        b'\x02' * 32,
    ),
)
def test_public_key_edge_cases_match_iroh(key: bytes) -> None:
    _assert_matches_iroh(key)


def test_public_keys_match_iroh() -> None:
    for _ in range(2000):
        _assert_matches_iroh(os.urandom(32))


def _assert_matches_iroh(key: bytes) -> None:
    try:
        iroh.EndpointId.from_string(key.hex())
    except iroh.IrohError:
        iroh_valid = False
    else:
        iroh_valid = True

    try:
        EndpointId(key.hex())
    except ValueError:
        valid = False
    else:
        valid = True

    assert valid == iroh_valid, key.hex()


def test_pickle() -> None:
    endpoint_id = EndpointId.random()
    loaded = pickle.loads(pickle.dumps(endpoint_id))
    assert loaded == endpoint_id
    assert isinstance(loaded, EndpointId)


class _Model(pydantic.BaseModel):
    id: EndpointId


def test_pydantic() -> None:
    model = _Model.model_validate({'id': f' {_ID.upper()} '})
    assert isinstance(model.id, EndpointId)
    assert model.id == _ID
    assert _Model.model_validate({'id': _ID}, strict=True).id == _ID
    dumped = model.model_dump()
    assert dumped == {'id': _ID}
    assert type(dumped['id']) is str
    assert model.model_dump_json() == f'{{"id":"{_ID}"}}'

    with pytest.raises(pydantic.ValidationError, match='not a valid'):
        _Model.model_validate({'id': 'abc'})

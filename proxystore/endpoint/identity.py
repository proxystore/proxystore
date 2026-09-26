"""Endpoint identities.

Each endpoint has an ed25519 secret key which is stored in its directory.
The public key of the endpoint, its
[`EndpointId`][proxystore.endpoint.identity.EndpointId], identifies the
endpoint to clients and peer endpoints. Peers verify each other's
identity when establishing a connection because only the endpoint which has
the secret key can prove ownership of the public key.

Parsing endpoint IDs only depends on the standard library so clients do not
require any of the `endpoints` extra dependencies. Functions that operate
on secret keys require the `iroh` package which is included in the
`endpoints` extra.
"""

from __future__ import annotations

import re
from typing import NewType

EndpointId = NewType('EndpointId', str)
"""ID of an endpoint.

The ID is the endpoint's ed25519 public key encoded as 64 lowercase
hexadecimal characters.
"""

SECRET_KEY_SIZE = 32
"""Size in bytes of an endpoint secret key."""

_ENDPOINT_ID_PATTERN = re.compile(r'[0-9a-f]{64}')


def parse_endpoint_id(value: str) -> EndpointId:
    """Parse an endpoint ID.

    Args:
        value: Endpoint ID encoded as 64 hexadecimal characters. Uppercase
            characters are converted to lowercase.

    Returns:
        The endpoint ID.

    Raises:
        ValueError: If `value` is not a valid endpoint ID.
    """
    if isinstance(value, str):
        id_ = value.strip().lower()
        if _ENDPOINT_ID_PATTERN.fullmatch(id_):
            return EndpointId(id_)
    raise ValueError(
        f'"{value}" is not a valid endpoint ID. An endpoint ID is 64 '
        'hexadecimal characters.',
    )


def short_id(endpoint_id: EndpointId) -> str:
    """Get a short prefix of an endpoint ID for logging."""
    return endpoint_id[:10]


def log_name(endpoint_id: EndpointId, name: str) -> str:
    """Return string formatted as `#!python 'name(id-prefix)'`."""
    return f'{name}({short_id(endpoint_id)})'


def generate_secret_key() -> bytes:
    """Generate a new endpoint secret key.

    Returns:
        The secret key as
        [`SECRET_KEY_SIZE`][proxystore.endpoint.identity.SECRET_KEY_SIZE]
        bytes.
    """
    import iroh

    return iroh.SecretKey.generate().to_bytes()


def endpoint_id_from_secret_key(secret_key: bytes) -> EndpointId:
    """Get the ID of the endpoint with the secret key.

    Raises:
        ValueError: If `secret_key` is not a valid secret key.
    """
    import iroh

    if len(secret_key) != SECRET_KEY_SIZE:
        raise ValueError(
            f'Endpoint secret key must be {SECRET_KEY_SIZE} bytes but got '
            f'{len(secret_key)} bytes.',
        )
    public = iroh.SecretKey.from_bytes(secret_key).public()
    return parse_endpoint_id(str(public))

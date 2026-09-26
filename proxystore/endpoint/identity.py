"""Endpoint identities.

Each endpoint has an ed25519 secret key which is stored in its directory.
The public key of the endpoint, its
[`EndpointId`][proxystore.endpoint.identity.EndpointId], identifies the
endpoint to clients and peer endpoints. Peers verify each other's
identity when establishing a connection because only the endpoint which has
the secret key can prove ownership of the public key.

Parsing endpoint IDs only depends on the standard library so clients do not
require any of the `endpoints` extra dependencies. Operations on keys (e.g.,
[`EndpointId.from_secret_key()`][proxystore.endpoint.identity.EndpointId.from_secret_key])
require the `iroh` package which is included in the `endpoints` extra.
"""

from __future__ import annotations

import functools
import re
from typing import Any
from typing import Self
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler
    from pydantic_core import CoreSchema

SECRET_KEY_SIZE = 32
"""Size in bytes of an endpoint secret key."""

_ENDPOINT_ID_PATTERN = re.compile(r'[0-9a-f]{64}')

# Parameters of the edwards25519 curve (RFC 8032).
_P = 2**255 - 19
_D = (-121665 * pow(121666, -1, _P)) % _P


@functools.lru_cache(maxsize=1024)
def _is_public_key(value: str) -> bool:
    """Check if a hex-encoded value is a valid ed25519 public key.

    Implements point decoding from RFC 8032 (Section 5.1.3) so that IDs can
    be validated without `iroh`. To match the ed25519 implementation used by
    iroh, non-canonical encodings of the y-coordinate and a set sign bit
    when x is zero are accepted, which RFC 8032 rejects.
    """
    y = int.from_bytes(bytes.fromhex(value), 'little')
    # The sign bit of x does not affect whether the point exists.
    y = (y & ((1 << 255) - 1)) % _P
    u = (y * y - 1) % _P
    v = (_D * y * y + 1) % _P
    # Candidate square root of u / v.
    x = (u * pow(v, 3, _P) * pow(u * pow(v, 7, _P), (_P - 5) // 8, _P)) % _P
    vx2 = (v * x * x) % _P
    # If neither x nor x * sqrt(-1) is a root, u / v is not a square so
    # there is no point with this y.
    return vx2 in (u, (-u) % _P)


class EndpointId(str):
    """ID of an endpoint.

    The ID is the endpoint's ed25519 public key encoded as 64 lowercase
    hexadecimal characters. An `EndpointId` is a `str` so it can be used
    anywhere a string is expected (e.g., serialized in configuration files).
    Every instance is a valid public key (not every 32-byte value is), and
    validation only depends on the standard library.
    The constructor only accepts the exact format. Use
    [`from_str()`][proxystore.endpoint.identity.EndpointId.from_str] to also
    accept surrounding whitespace and uppercase characters.

    Example:
        ```python
        endpoint_id = EndpointId.from_str('4C1D...')
        assert endpoint_id == '4c1d...'
        print(endpoint_id.log_name('my-endpoint'))  # my-endpoint(4c1d...)
        ```

    Raises:
        ValueError: If the value is not a valid endpoint ID.
    """

    __slots__ = ()

    def __new__(cls, value: str) -> Self:  # noqa: D102
        if not isinstance(value, str) or not _ENDPOINT_ID_PATTERN.fullmatch(
            value,
        ):
            raise ValueError(
                f'"{value}" is not a valid endpoint ID. An endpoint ID is 64 '
                'hexadecimal characters.',
            )
        if not _is_public_key(value):
            raise ValueError(
                f'"{value}" is not a valid endpoint ID because it is not a '
                'valid public key.',
            )
        return super().__new__(cls, value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({str(self)!r})'

    @classmethod
    def from_str(cls, value: str) -> Self:
        """Parse an endpoint ID.

        Args:
            value: Endpoint ID encoded as 64 hexadecimal characters.
                Surrounding whitespace is removed and uppercase characters
                are converted to lowercase.

        Raises:
            ValueError: If `value` is not a valid endpoint ID.
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            value = value.strip().lower()
        return cls(value)

    @classmethod
    def from_secret_key(cls, secret_key: bytes) -> Self:
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
        return cls(str(iroh.SecretKey.from_bytes(secret_key).public()))

    @classmethod
    def random(cls) -> Self:
        """Generate the ID of a new random endpoint.

        The secret key of the endpoint is discarded so this is only useful
        when an ID is needed but the endpoint is not (e.g., in tests).
        """
        return cls.from_secret_key(generate_secret_key())

    def short(self) -> str:
        """Get a short prefix of the ID for logging."""
        return self[:10]

    def log_name(self, name: str) -> str:
        """Format the ID with a name as `#!python 'name(id-prefix)'`."""
        return f'{name}({self.short()})'

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        from pydantic_core import core_schema

        return core_schema.no_info_plain_validator_function(
            cls.from_str,
            serialization=core_schema.to_string_ser_schema(when_used='always'),
        )


def generate_secret_key() -> bytes:
    """Generate a new endpoint secret key.

    Returns:
        The secret key as
        [`SECRET_KEY_SIZE`][proxystore.endpoint.identity.SECRET_KEY_SIZE]
        bytes.
    """
    import iroh

    return iroh.SecretKey.generate().to_bytes()

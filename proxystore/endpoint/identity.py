"""Endpoint identities.

Each endpoint has an ed25519 secret key which is stored in its directory.
The public key of the endpoint, its
[`EndpointId`][proxystore.endpoint.identity.EndpointId], identifies the
endpoint to clients and peer endpoints. Peers verify each other's
identity when establishing a connection because only the endpoint which has
the secret key can prove ownership of the public key.

"""

from __future__ import annotations

import functools
import hmac
import re
from typing import Any
from typing import Self

import iroh
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from pydantic_core import CoreSchema

_ENDPOINT_ID_PATTERN = re.compile(r'[0-9a-f]{64}')


@functools.lru_cache(maxsize=1024)
def _is_public_key(value: str) -> bool:
    try:
        iroh.EndpointId.from_string(value)
    except iroh.IrohError:
        return False
    return True


class EndpointId(str):
    """ID of an endpoint.

    The ID is the endpoint's ed25519 public key encoded as 64 lowercase
    hexadecimal characters. An `EndpointId` is a `str` so it can be used
    anywhere a string is expected (e.g., serialized in configuration files).
    Every instance is a valid public key (not every 32-byte value is).
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
    def random(cls) -> Self:
        """Generate the ID of a new random endpoint.

        The secret key of the endpoint is discarded so this is only useful
        when an ID is needed but the endpoint is not (e.g., in tests).
        """
        return cls(SecretKey.generate().endpoint_id)

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
        return core_schema.no_info_plain_validator_function(
            cls.from_str,
            serialization=core_schema.to_string_ser_schema(when_used='always'),
        )


class SecretKey:
    """Secret key of an endpoint.

    The secret key is an ed25519 key which proves the identity of the
    endpoint to peers. The key is never included in its `repr()` so it is not
    accidentally logged.

    Example:
        ```python
        secret_key = SecretKey.generate()
        endpoint_id = secret_key.endpoint_id
        same_key = SecretKey(secret_key.to_bytes())
        ```

    Args:
        key: Raw bytes of the secret key.

    Raises:
        ValueError: If `key` is not a valid secret key.
    """

    __slots__ = ('_endpoint_id', '_key')

    def __init__(self, key: bytes) -> None:
        try:
            public = iroh.SecretKey.from_bytes(key).public()
        except iroh.IrohError:
            raise ValueError(
                f'Endpoint secret key is not a valid ed25519 secret key '
                f'({len(key)} bytes).',
            ) from None
        self._key = bytes(key)
        self._endpoint_id = EndpointId(str(public))

    def __repr__(self) -> str:
        return f'{type(self).__name__}(endpoint_id={self.endpoint_id!r})'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SecretKey):
            return NotImplemented
        return hmac.compare_digest(self._key, other._key)

    def __hash__(self) -> int:
        return hash(self.endpoint_id)

    @classmethod
    def generate(cls) -> Self:
        """Generate a new random secret key."""
        return cls(iroh.SecretKey.generate().to_bytes())

    @property
    def endpoint_id(self) -> EndpointId:
        """ID of the endpoint with this secret key (i.e., the public key)."""
        return self._endpoint_id

    def to_bytes(self) -> bytes:
        """Get the raw bytes of the secret key."""
        return self._key

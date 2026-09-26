"""Mock implementation of PyDAOS.

PyDAOS is not available on PyPI so this module is injected in place of the
`pydaos` package by `tests/conftest.py` when PyDAOS is not installed.

Much of this code mirrors the reference implementation where possible so
some of the choices are a bit strange.

Reference implementation:
https://github.com/daos-stack/daos/blob/release/2.4/src/client/pydaos/pydaos_core.py
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any


class DObjNotFound(Exception):  # noqa: N818
    """Raised by get if name associated with DAOS object not found."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(self)

    def __str__(self) -> str:
        return f'Failed to open "{self.name}"'


class DCont:
    """Class representing a DAOS Python container."""

    def __init__(
        self,
        pool: str | None = None,
        cont: str | None = None,
        path: str | None = None,
    ) -> None:
        if path is not None:
            raise ValueError(
                'The mock DCont from PyDAOS does not support path. '
                'Use the pool and cont arguments instead.',
            )
        if pool is None or cont is None:
            raise ValueError('Both pool and cont must be provided.')

        self._pool = pool
        self._cont = cont
        self._dobjs: dict[str, _DObj] = {}

    def __getitem__(self, name: str) -> _DObj:
        return self.get(name)

    def __str__(self) -> str:
        return f'{self._pool}/{self._cont}'

    def __repr__(self) -> str:
        return f'daos://{self._pool}/{self._cont}'

    def get(self, name: str) -> _DObj:
        """Get an object by name."""
        obj = self._dobjs.get(name, None)
        if obj is None:
            raise DObjNotFound(name)
        return obj

    def dict(
        self,
        name: str,
        v: dict[str, bytes | None] | None = None,
        cid: str = '0',
    ) -> DDict:
        """Create a new DAOS dictionary."""
        dd = DDict(name)
        dd.bput(v)
        self._dobjs[name] = dd
        return dd


class _DObj:
    pass


class DDict(_DObj):
    """Class representing of DAOS dictionary.

    Keys are strings and values are byte strings only. Deleting a key is
    implemented by setting the value to `None`.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.value_size = 1000 * 1000
        self._data: dict[str, bytes | None] = {}

    def __delitem__(self, key: str) -> None:
        self.put(key, None)

    def __getitem__(self, key: str) -> bytes:
        return self.get(key)

    def __setitem__(self, key: str, val: bytes) -> None:
        self.put(key, val)

    def __len__(self) -> int:
        return len(self.dump())

    def __bool__(self) -> bool:
        return len(self) > 0

    def __contains__(self, key: str) -> bool:
        try:
            self.get(key)
        except KeyError:
            return False
        return True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DDict):
            return False
        return self.dump() == other.dump()

    def __hash__(self) -> int:
        raise NotImplementedError

    def __iter__(self) -> Generator[str, None, None]:
        for key, value in self._data.items():
            if value is not None:
                yield key

    def get(self, key: str) -> bytes:
        """Get a value by key."""
        val = self._data.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def put(self, key: str, val: bytes | None) -> None:
        """Put a value in the dictionary."""
        self.bput({key: val})

    def pop(self, key: str) -> None:
        """Remove a value from the dictionary."""
        self.put(key, None)

    def bget(
        self,
        d: dict[str, Any] | None,
        value_size: int | None = None,
    ) -> dict[str, bytes] | None:
        """Bulk get values from the dictionary.

        Raises:
            KeyError: If any key is missing.
        """
        if d is None:
            return None
        for k in d:
            d[k] = self.get(k)
        return d

    def bput(self, d: dict[str, bytes | None] | None) -> None:
        """Bulk put values in the dictionary."""
        if d is None:
            return
        self._data.update(d)

    def dump(self) -> dict[str, bytes]:
        """Dump all key value pairs."""
        res = self.bget(dict.fromkeys(self))
        assert res is not None
        return res

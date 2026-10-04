"""Compatibility shims for the v1 store registration API.

ProxyStore v2 registers every store when it is created and unregisters it
when it is closed, so the v1 registration functions are no longer needed.
They are kept here as deprecated no-ops so that code written for v1 keeps
working. They are re-exported by `proxystore.store` and
`proxystore.store.exceptions` but intentionally left out of the docs.

To remove these shims (e.g., in the next major version):

1. Delete this file and `tests/store/compat_test.py`.
2. In `proxystore/store/__init__.py`, remove the imports from
   `proxystore.store._compat` and the `@ignore_register_kwarg` decorator
   on `get_or_create_store()`.
3. In `proxystore/store/exceptions.py`, remove the module-level
   `__getattr__()` that returns `StoreExistsError`.
4. Search for leftovers. This should find nothing.

   ```
   grep -rnF -e proxystore.store._compat -e StoreExistsError proxystore tests
   ```

Keep the following, which are not part of the shims:

* `proxystore/_compat.py`, which handles compatibility between 2.x
  versions and is unrelated to this module.
* The `TypeError` raised by `get_store()` when given a string. It is a
  helpful error for any misuse, not only v1 code.
* The `base` import under `TYPE_CHECKING` in
  `proxystore/store/exceptions.py`. It avoids an import cycle.
"""

from __future__ import annotations

import contextlib
import functools
import warnings
from collections.abc import Callable
from typing import Any
from typing import Protocol
from typing import TYPE_CHECKING

from proxystore.store.exceptions import StoreError

if TYPE_CHECKING:
    from proxystore.store.base import Store
    from proxystore.store.config import StoreConfig


def _warn(message: str) -> None:
    warnings.warn(
        f'{message} It will be removed in the next major version.',
        DeprecationWarning,
        stacklevel=3,
    )


class StoreExistsError(StoreError):
    """Deprecated. This exception is no longer raised."""


def register_store(store: Store[Any], exist_ok: bool = False) -> None:
    """Deprecated. Stores are registered when created."""
    _warn('register_store() is deprecated and does nothing.')


def unregister_store(name_or_store: str | Store[Any]) -> None:
    """Deprecated. Stores are unregistered when closed."""
    _warn('unregister_store() is deprecated and does nothing.')


def store_registration(
    *stores: Store[Any],
    exist_ok: bool = False,
) -> contextlib.nullcontext[None]:
    """Deprecated. Stores are registered when created."""
    _warn('store_registration() is deprecated and does nothing.')
    return contextlib.nullcontext()


class _GetOrCreateStore(Protocol):
    # The v1 signature of get_or_create_store(). Type checkers see this
    # signature so code that supports v1 and v2 type checks with both.
    def __call__(
        self,
        store_config: StoreConfig,
        *,
        register: bool = True,
    ) -> Store[Any]: ...


def ignore_register_kwarg(
    function: Callable[[StoreConfig], Store[Any]],
) -> _GetOrCreateStore:
    """Drop the deprecated `register` argument of `get_or_create_store()`."""

    @functools.wraps(function)
    def _wrapper(
        store_config: StoreConfig,
        *,
        register: bool | None = None,
    ) -> Store[Any]:
        if register is not None:
            _warn(
                'The register argument of get_or_create_store() is '
                'deprecated and does nothing.',
            )
        return function(store_config)

    return _wrapper

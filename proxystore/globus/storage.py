"""Globus Auth token storage."""
from __future__ import annotations

import os
import pathlib

from globus_sdk.tokenstorage import SQLiteAdapter

from proxystore.utils.environment import home_dir

_TOKENS_FILE = 'storage.db'


def get_token_storage_adapter(
    filepath: str | None = None,
    *,
    namespace: str = 'DEFAULT',
) -> SQLiteAdapter:
    """Create token storage adapter.

    Args:
        filepath: Name of the database file. This is passed to SQLite so
            `:memory:` is a valid option for an in-memory database. If not
            provided, defaults to a file in the ProxyStore home directory
            (see [`home_dir()`][proxystore.utils.environment.home_dir]).
        namespace: Optional namespace to use within the database. See
            [`SQLiteAdapter`][globus_sdk.tokenstorage.SQLiteAdapter] for
            more detauls.

    Returns:
        Token storage adapter.
    """
    if filepath is None:
        filepath = os.path.join(home_dir(), _TOKENS_FILE)
    if filepath != ':memory:':
        pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    return SQLiteAdapter(filepath, namespace=namespace)

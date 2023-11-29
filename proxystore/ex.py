"""Extension modules.

This module re-exports packages and modules from the
[`proxystore-ex`](https://extensions.proxystore.dev){target=_blank} package,
and is the recommended method by which to import features from the extensions
package.

Example:
    Extension features can be imported directly. E.g.,
    ```python
    from proxystore_ex.connectors.daos import DAOSConnector
    ```
    But we recommend replacing `proxystore_ex` with `proxystore.ex`. E.g.,
    ```python
    from proxystore.ex.connectors.daos import DAOSConnector
    ```

The API reference for the extensions package can be found at
[extensions.proxystore.dev/latest/api](https://extensions.proxystore.dev/latest/api/){target=_blank}.

Warning:
    A import error will be raised if `proxystore-ex` is not installed.
    See the [Installation](../installation.md) page for installation
    instructions.
"""
from __future__ import annotations

from typing import Any

_import_error_message = """\
The proxystore_ex package is not installed.

Install directly or with the extensions option when installing ProxyStore.
  pip install proxystore[extensions]
  pip install proxystore-ex\
"""

# This method is based on how dask imports the distribtued package to
# be accessible via the dask.distributed subpackage. Source:
# https://github.com/dask/dask/blob/b2f11d026d2c6f806036c050ff5dbd59d6ceb6ec/dask/distributed.py
try:
    from proxystore_ex import *  # noqa: F403
except ImportError as e:  # pragma: no cover
    if e.msg == "No module named 'proxystore_ex'":
        raise ImportError(_import_error_message) from e
    else:
        raise


def __getattr__(value: str) -> Any:
    try:
        import proxystore_ex
    except ImportError as e:  # pragma: no cover
        raise ImportError(_import_error_message) from e
    return getattr(proxystore_ex, value)

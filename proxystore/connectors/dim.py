"""Distributed in-memory store connectors.

Warning:
    The distributed in-memory connector implementations have moved to the
    [ProxyStore Extensions](https://extensions.proxystore.dev) package as
    of ProxyStore v0.6.0. To update, install the `proxystore-ex` package
    ```bash
    $ pip install proxystore-ex
    ```
    and change the imports accordingly. E.g.,
    ```python
    from proxystore.connectors.dim.zmq import ZeroMQConnector  # OLD
    from proxystore_ex.connectors.dim.zmq import ZeroMQConnector  # NEW
    ```
"""
from __future__ import annotations

import warnings

warnings.warn(
    """\
The proxystore.connectors.dim module has moved to ProxyStore Extensions.

To update, install the Extensions package

$ pip install proxystore-ex

and change imports from "proxystore.connectors.dim" to "proxystore_ex.connectors.dim."\
""",
    stacklevel=2,
)

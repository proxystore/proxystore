from __future__ import annotations

import pytest


def test_warning():
    with pytest.warns(
        UserWarning,
        match=(
            'The proxystore.connectors.dim module has moved to '
            'ProxyStore Extensions.'
        ),
    ):
        import proxystore.connectors.dim  # noqa: F401

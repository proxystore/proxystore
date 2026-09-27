from __future__ import annotations

import os
import subprocess
import sys
import textwrap

from proxystore.endpoint.config import EndpointConfig
from proxystore.endpoint.directory import EndpointDir

# Dependencies of the endpoints extra which clients must not require.
_EXTRAS = ('aiosqlite', 'cryptography', 'daemon', 'iroh', 'uvloop')


def test_clients_do_not_require_endpoints_extra(
    endpoint: EndpointConfig,
    endpoint_dir: EndpointDir,
) -> None:
    code = textwrap.dedent(
        f"""
        import sys

        class _BlockExtras:
            def find_spec(self, name, path=None, target=None):
                if name.split('.')[0] in {_EXTRAS!r}:
                    raise ImportError(f'{{name}} is blocked')
                return None

        sys.meta_path.insert(0, _BlockExtras())

        from proxystore.connectors.endpoint import EndpointConnector
        from proxystore.endpoint.identity import EndpointId

        # Validating an ID does not require iroh
        endpoint_id = EndpointId.from_str({endpoint.id!r})
        with EndpointConnector(
            [endpoint_id],
            proxystore_dir={os.path.dirname(endpoint_dir.path)!r},
        ) as connector:
            key = connector.put(b'value')
            assert connector.get(key) == b'value'
            connector.evict(key)

        blocked = [
            name for name in sys.modules if name.split('.')[0] in {_EXTRAS!r}
        ]
        assert not blocked, blocked
        """,
    )
    result = subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr

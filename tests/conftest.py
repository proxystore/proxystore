"""Public fixtures for unit tests."""
from __future__ import annotations

import sys

try:
    import pymargo
except ImportError:
    from testing.mocked import pymargo

    sys.modules['pymargo'] = pymargo
    sys.modules['pymargo.bulk'] = pymargo
    sys.modules['pymargo.core'] = pymargo

try:
    import ucp
except ImportError:
    from testing.mocked import ucx

    sys.modules['ucp'] = ucx


from testing.signaling_server import signaling_server
from testing.store_utils import endpoint_store
from testing.store_utils import file_store
from testing.store_utils import globus_store
from testing.store_utils import local_store
from testing.store_utils import margo_store
from testing.store_utils import redis_store
from testing.store_utils import ucx_store
from testing.store_utils import websocket_store
from testing.utils import tmp_dir

# Import fixtures from testing/ so they are known by pytest
# and can be used with

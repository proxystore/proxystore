"""Store Operation Statistics Examples.

Source code for:
    https://proxystore.readthedocs.io/en/latest/guides/performance.html
"""
from __future__ import annotations

import tempfile
from pprint import pprint

from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store

fp = tempfile.TemporaryDirectory()

store = Store('example', FileConnector(fp.name), stats=True)

target = list(range(0, 100))
key = store.set(target)

stats = store.stats(key)
print(stats.keys())
pprint(stats['store_set'])
pprint(stats['connector_put'])

store.get(key)

stats = store.stats(key)
print(stats.keys())

# Attributes of `TimeStats` can be accessed directly
print(stats['store_get'].calls)
pprint(stats['store_get'])

# Check that the avg time of `get` decreases due to caching
# when called twice in a row.
store.get(key)
stats = store.stats(key)
print(stats['store_get'].calls)
pprint(stats['store_get'])

# Access stats with a proxy
target_proxy = store.proxy(target)
stats = store.stats(target_proxy)
print(stats.keys())
pprint(stats['store_proxy'])

# Access the proxy to force it to resolve
assert target_proxy[0] == 0

stats = store.stats(target_proxy)
print(stats.keys())
pprint(stats['factory_resolve'])

store.close()
fp.cleanup()

"""Store Operation Statistics Examples.

Source code for:
    https://proxystore.readthedocs.io/en/latest/guides/performance.html
"""
from __future__ import annotations

import tempfile
from typing import Any

from proxystore.proxy import Proxy
from proxystore.store.file import FileStore

fp = tempfile.TemporaryDirectory()

store = FileStore(name='default', store_dir=fp.name, stats=True)

target = list(range(0, 100))
key1 = store.set(target)
key2 = store.set(target)

stats1 = store.stats(key1)
stats2 = store.stats(key2)
print(
    f"""\
stats1 = store.stats(key1)
--------------------------
  stats1.keys() = {stats1.keys()}
  stats1['set'] = {stats1['set']}
  stats1['set'].calls = {stats1['set'].calls}
  stats1['set'].avg_time_ms = {stats1['set'].avg_time_ms}
  stats1['set_bytes'].size_bytes = {stats1['set_bytes'].size_bytes}
""",
)
print(
    f"""\
store2 = store.stats(key2)
--------------------------
  stats2.keys() = {stats2.keys()}
  stats2['set'] = {stats2['set']}
  stats2['set'].calls = {stats2['set'].calls}
  stats2['set'].avg_time_ms = {stats2['set'].avg_time_ms}
  stats2['set_bytes'].size_bytes = {stats2['set_bytes'].size_bytes}
""",
)

print(stats1['set'], stats1['set_bytes'])

print('Call store.get(key1)')
store.get(key1)
stats = store.stats(key1)
print(
    f"""\
store = store.stats(key1)
-------------------------
  stats.keys() = {stats.keys()}
  stats['get'] = {stats['get']}
  stats['get'].calls = {stats['get'].calls}
  stats['get'].avg_time_ms = {stats['get'].avg_time_ms}
  stats['get_bytes'].size_bytes = {stats['get_bytes'].size_bytes}
""",
)

print('Call store.get(key1) again')
store.get(key1)
stats = store.stats(key1)
print(
    f"""\
store = store.stats(key1)
-------------------------
  stats.keys() = {stats.keys()}
  stats['get'] = {stats['get']}
  stats['get'].calls = {stats['get'].calls}
  stats['get'].avg_time_ms = {stats['get'].avg_time_ms}
""",
)

target_proxy: Proxy[Any] = store.proxy(target)
stats = store.stats(target_proxy)
print(
    f"""\
store = store.stats(target_proxy)
---------------------------------
  stats.keys() = {stats.keys()}
  stats['set'] = {stats['set']}
  stats['proxy'] = {stats['proxy']}
""",
)

assert target_proxy[0] == 0
stats = store.stats(target_proxy)
print(
    f"""\
store = store.stats(target_proxy)
---------------------------------
  stats.keys() = {stats.keys()}
  stats['resolve'] = {stats['resolve']}
  stats['resolve'].calls = {stats['resolve'].calls}
  stats['resolve'].avg_time_ms = {stats['resolve'].avg_time_ms}
""",
)

store.close()
fp.cleanup()

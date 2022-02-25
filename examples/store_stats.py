"""Store Operation Statistics Examples."""
from __future__ import annotations

import proxystore as ps

store = ps.store.init_store(
    'file',
    name='default',
    store_dir='/tmp/proxystore-dump',
    stats=True,
)

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
""",
)

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

target_proxy = store.proxy(target)
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

store.cleanup()

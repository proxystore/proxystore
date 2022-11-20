"""FuncX and ProxyStore example."""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import numpy as np
from funcx.sdk.client import FuncXClient

from proxystore.store import register_store
from proxystore.store.base import Store
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.store.redis import RedisStore

# Note: types on function are not provided because FuncX has trouble
# serializing them sometimes


def app_double(x):  # type: ignore
    """Doubles input array."""
    return 2 * x


def app_sum(inputs):  # type: ignore
    """Sum all elements in list of arrays."""
    import numpy as np

    if len(inputs) == 0:
        return 0

    out = inputs[0]
    for x in inputs[1:]:
        out += x
    return np.sum(out)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='MapReduce with FuncX and ProxyStore',
    )
    parser.add_argument(
        '-e',
        '--endpoint',
        required=True,
        help='FuncX endpoint for task execution',
    )
    parser.add_argument(
        '-n',
        '--num-arrays',
        type=int,
        default=10,
        help='Number of arrays to be mapreduced',
    )
    parser.add_argument(
        '-s',
        '--size',
        type=int,
        default=100,
        help='Length of array where each array has shape [s, s]',
    )

    ps_group = parser.add_argument_group()
    ps_backend_group = ps_group.add_mutually_exclusive_group(required=False)
    ps_backend_group.add_argument(
        '--ps-file',
        action='store_true',
        help='Use the ProxyStore file backend.',
    )
    ps_backend_group.add_argument(
        '--ps-globus',
        action='store_true',
        help='Use the ProxyStore Globus backend.',
    )
    ps_backend_group.add_argument(
        '--ps-redis',
        action='store_true',
        help='Use the ProxyStore Redis backend.',
    )
    ps_group.add_argument(
        '--ps-file-dir',
        required='--ps-file' in sys.argv,
        help='Temp directory to store proxied object in.',
    )
    ps_group.add_argument(
        '--ps-globus-config',
        required='--ps-globus' in sys.argv,
        help='Globus Endpoint config file to use with ProxyStore.',
    )
    ps_group.add_argument(
        '--ps-redis-port',
        type=int,
        required='--ps-redis' in sys.argv,
        help=(
            'Redis server running on the localhost ' 'to use with ProxyStore '
        ),
    )
    args = parser.parse_args()

    fxc = FuncXClient()

    double_uuid = fxc.register_function(app_double)
    sum_uuid = fxc.register_function(app_sum)

    store: Store[Any] | None = None
    if args.ps_file:
        store = FileStore('file', store_dir=args.ps_file_dir)
    elif args.ps_globus:
        endpoints = GlobusEndpoints.from_json(args.ps_globus_config)
        store = GlobusStore('globus', endpoints=endpoints)
    elif args.ps_redis:
        store = RedisStore(
            'redis',
            hostname='localhost',
            port=args.ps_redis_port,
        )

    if store is not None:
        register_store(store)

    start = time.perf_counter()

    results = []
    for _ in range(args.num_arrays):
        x = np.random.rand(args.size, args.size)
        if store is not None:
            x = store.proxy(x)
        results.append(
            fxc.run(
                x,
                endpoint_id=args.endpoint,
                function_id=double_uuid,
            ),
        )

    for result in results:
        while fxc.get_task(result)['pending']:
            time.sleep(0.1)

    mapped_results = [fxc.get_result(result) for result in results]

    if store is not None:
        mapped_results = store.proxy(mapped_results)
    total = fxc.run(
        mapped_results,
        endpoint_id=args.endpoint,
        function_id=sum_uuid,
    )

    while fxc.get_task(total)['pending']:
        time.sleep(0.1)

    print(f'Sum: {fxc.get_result(total)}')
    print(f'Time: {time.perf_counter() - start:.2f}')

    if store is not None:
        if hasattr(store, 'cleanup'):
            store.cleanup()
        elif hasattr(store, 'close'):
            store.close()

"""Globus Compute and ProxyStore example."""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import numpy as np
from globus_compute_sdk import Executor

from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.redis import RedisConnector
from proxystore.store import register_store
from proxystore.store.base import Store


def app_double(x: np.ndarray) -> np.ndarray:
    """Doubles input array."""
    return 2 * x


def app_sum(inputs: np.ndarray) -> np.ndarray:
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
        description='MapReduce with Globus Compute and ProxyStore',
    )
    parser.add_argument(
        '-e',
        '--endpoint',
        required=True,
        help='Globus Compute endpoint for task execution',
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
        help='Redis server running on the localhost to use with ProxyStore',
    )
    args = parser.parse_args()

    store: Store[Any] | None = None
    if args.ps_file:
        store = Store('file', FileConnector(store_dir=args.ps_file_dir))
    elif args.ps_globus:
        endpoints = GlobusEndpoints.from_json(args.ps_globus_config)
        store = Store('globus', GlobusConnector(endpoints=endpoints))
    elif args.ps_redis:
        store = Store('redis', RedisConnector('localhost', args.ps_redis_port))

    if store is not None:
        register_store(store)

    start = time.perf_counter()

    with Executor(endpoint_id=args.endpoint) as gce:
        futures = []
        for _ in range(args.num_arrays):
            x = np.random.rand(args.size, args.size)
            if store is not None:
                x = store.proxy(x)
            futures.append(gce.submit(app_double, x))

        mapped_results = [future.result() for future in futures]

        if store is not None:
            mapped_results = store.proxy(mapped_results)
        total = gce.submit(app_sum, mapped_results).result()

    print(f'Sum: {total}')
    print(f'Time: {time.perf_counter() - start:.2f}')

    if store is not None:
        store.close()

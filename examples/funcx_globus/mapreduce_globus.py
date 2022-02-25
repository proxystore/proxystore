"""FuncX and ProxyStore+Globus example."""
from __future__ import annotations

import argparse
import time

import numpy as np
from funcx.sdk.client import FuncXClient

import proxystore as ps


def app_double(x: np.ndarray) -> np.ndarray:
    """Doubles input array."""
    return 2 * x


def app_sum(inputs: list[np.ndarray]) -> float:
    """Sum all elements in list of arrays."""
    import numpy as np

    if len(inputs) == 0:
        return 0

    out = inputs[0]
    for x in inputs[1:]:
        out += x
    return np.sum(out)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce with FuncX')
    parser.add_argument(
        '-n',
        '--num-arrays',
        type=int,
        required=True,
        help='Number of arrays to be mapreduced',
    )
    parser.add_argument(
        '-s',
        '--size',
        type=int,
        required=True,
        help='Length of array where each array is s x s',
    )
    parser.add_argument(
        '--remote-funcx-endpoint',
        type=str,
        required=True,
        help='FuncX endpoint ID',
    )
    parser.add_argument(
        '--globus-config-file',
        type=str,
        required=True,
        help='Path to JSON file with Globus endpoints',
    )
    parser.add_argument(
        '--proxy',
        action='store_true',
        help='Use proxy store to pass inputs',
    )
    args = parser.parse_args()

    fxc = FuncXClient()

    double_uuid = fxc.register_function(app_double)
    sum_uuid = fxc.register_function(app_sum)

    if args.proxy:
        endpoints = ps.store.globus.GlobusEndpoints.from_json(
            args.globus_config_file,
        )
        store = ps.store.init_store(
            'globus',
            name='globus',
            endpoints=endpoints,
        )

    batch = fxc.create_batch()
    for _ in range(args.num_arrays):
        x = np.random.rand(args.size, args.size)
        if args.proxy:
            x = store.proxy(x)
        batch.add(
            x,
            endpoint_id=args.remote_funcx_endpoint,
            function_id=double_uuid,
        )

    batch_res = fxc.batch_run(batch)
    for res in batch_res:
        while fxc.get_task(res)['pending']:
            time.sleep(0.5)

    mapped_results = [fxc.get_result(task_id) for task_id in batch_res]

    if args.proxy:
        mapped_results = store.proxy(mapped_results)
    total = fxc.run(
        mapped_results,
        endpoint_id=args.remote_funcx_endpoint,
        function_id=sum_uuid,
    )

    while fxc.get_task(total)['pending']:
        time.sleep(0.5)

    print('Sum:', fxc.get_result(total))

    if args.proxy:
        store.cleanup()

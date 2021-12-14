"""FuncX and ProxyStore+Globus example"""
import argparse
import numpy as np
import proxystore as ps
import time

from funcx.sdk.client import FuncXClient
from typing import List


def app_double(x: np.ndarray) -> np.ndarray:
    """Doubles input array"""
    return 2 * x


def app_sum(inputs: List[np.ndarray]) -> float:
    """Sums all elements in list of arrays"""
    import numpy as np

    if len(inputs) == 0:
        return

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
        '--local-globus-endpoint',
        type=str,
        required=True,
        help='UUID of local Globus endpoint'
    )
    parser.add_argument(
        '--local-globus-host',
        type=str,
        required=True,
        help='Regex for hostname of local Globus endpoint'
    )
    parser.add_argument(
        '--local-globus-endpoint-path',
        type=str,
        required=True,
        help='Path to dir within local Globus endpoint'
    )
    parser.add_argument(
        '--local-globus-local-path',
        type=str,
        required=True,
        help='Path to dir within local filesystem'
    )
    parser.add_argument(
        '--remote-globus-endpoint',
        type=str,
        required=True,
        help='UUID of remote Globus endpoint'
    )
    parser.add_argument(
        '--remote-globus-host',
        type=str,
        required=True,
        help='Regex for hostname of remote Globus endpoint'
    )
    parser.add_argument(
        '--remote-globus-endpoint-path',
        type=str,
        required=True,
        help='Path to dir within remote Globus endpoint'
    )
    parser.add_argument(
        '--remote-globus-local-path',
        type=str,
        required=True,
        help='Path to dir within remote Globus endpoint'
    )
    parser.add_argument(
        '--proxy', action='store_true', help='Use proxy store to pass inputs'
    )
    args = parser.parse_args()

    fxc = FuncXClient()

    double_uuid = fxc.register_function(app_double)
    sum_uuid = fxc.register_function(app_sum)

    if args.proxy:
        local_endpoint = ps.store.globus.GlobusEndpoint(
            args.local_globus_endpoint,
            args.local_globus_endpoint_path,
            args.local_globus_local_path,
            args.local_globus_host,
        )
        remote_endpoint = ps.store.globus.GlobusEndpoint(
            args.remote_globus_endpoint,
            args.remote_globus_endpoint_path,
            args.remote_globus_local_path,
            args.remote_globus_host,
        )
        store = ps.store.init_store(
            'globus', name='globus', endpoints=[local_endpoint, remote_endpoint] 
        )

    batch = fxc.create_batch()
    for i in range(args.num_arrays):
        x = np.random.rand(args.size, args.size)
        if args.proxy:
            x = store.proxy(x)
        batch.add(x, endpoint_id=args.remote_funcx_endpoint, function_id=double_uuid)

    batch_res = fxc.batch_run(batch)
    for res in batch_res:
        while fxc.get_task(res)['pending']:
            time.sleep(0.2)

    mapped_results = [
        fxc.get_result(i) for i, status in fxc.get_batch_result(batch_res).items()
    ]

    if args.proxy:
        mapped_results = store.proxy(mapped_results)
    total = fxc.run(
        mapped_results, endpoint_id=args.remote_funcx_endpoint, function_id=sum_uuid
    )

    while fxc.get_task(total)['pending']:
        time.sleep(0.2)

    print('Sum:', fxc.get_result(total))

    if args.proxy:
        store.cleanup()

"""MapReduce with FuncX and ProxyStore example"""
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
        '-f',
        '--funcx-endpoint',
        type=str,
        required=True,
        help='FuncX endpoint ID',
    )
    parser.add_argument(
        '--proxy', action='store_true', help='Use proxy store to pass inputs'
    )
    parser.add_argument(
        '--redis-port',
        type=int,
        default=59465,
        help='If not None, use Redis backend',
    )
    args = parser.parse_args()

    fxc = FuncXClient()

    double_uuid = fxc.register_function(app_double)
    sum_uuid = fxc.register_function(app_sum)

    if args.proxy:
        ps.init_redis_backend(hostname='127.0.0.1', port=args.redis_port)

    batch = fxc.create_batch()
    for i in range(args.num_arrays):
        x = np.random.rand(args.size, args.size)
        if args.proxy:
            x = ps.to_proxy(x)
        batch.add(x, endpoint_id=args.funcx_endpoint, function_id=double_uuid)

    batch_res = fxc.batch_run(batch)
    mapped_results = fxc.get_batch_result(batch_res)
    for i, res in mapped_results.items():
        while res['pending']:
            time.sleep(0.1)

    mapped_results = [
        fxc.get_result(i) for i, status in mapped_results.items()
    ]

    if args.proxy:
        mapped_results = ps.to_proxy(mapped_results)
    total = fxc.run(
        mapped_results, endpoint_id=args.funcx_endpoint, function_id=sum_uuid
    )

    while fxc.get_task(total)['pending']:
        time.sleep(0.1)

    print('Sum:', fxc.get_result(total))

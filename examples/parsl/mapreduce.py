"""MapReduce with Parsl and ProxyStore example"""
import argparse
import numpy as np
import parsl
import proxystore as ps

from typing import List
from parsl import python_app


@python_app
def app_double(x: np.ndarray) -> np.ndarray:
    """Doubles input array"""
    return 2 * x


@python_app
def app_sum(inputs: List[np.ndarray] = []) -> float:
    """Sums all elements in list of arrays"""
    return np.sum(np.sum(inputs))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce with Parsl')
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
        '--proxy', action='store_true', help='Use proxy store to pass inputs'
    )
    parser.add_argument(
        '--redis-port',
        type=int,
        default=None,
        help='If not None, use Redis backend',
    )
    args = parser.parse_args()

    parsl.load()

    if args.proxy:
        if args.redis_port is None:
            ps.init_local_backend()
        else:
            ps.init_redis_backend(hostname='127.0.0.1', port=args.redis_port)

    mapped_results = []
    for i in range(args.num_arrays):
        x = np.random.rand(args.size, args.size)
        if args.proxy:
            x = ps.to_proxy(x)
        mapped_results.append(app_double(x))

    total = app_sum(inputs=mapped_results)

    print('Sum:', total.result())

"""MapReduce with Parsl and ProxyStore example."""

from __future__ import annotations

import argparse
import contextlib
from typing import Any

import numpy as np
import parsl
from parsl import python_app

from proxystore.connectors.local import LocalConnector
from proxystore.connectors.protocols import Connector
from proxystore.connectors.redis import RedisConnector
from proxystore.store.base import Store


@python_app
def app_double(x: np.ndarray) -> np.ndarray:
    """Doubles input array."""
    return 2 * x


@python_app
def app_sum(inputs: list[np.ndarray] | None = None) -> float:
    """Sum all elements in list of arrays."""
    if inputs is not None:
        return np.sum(np.sum(inputs))
    return 0


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
        '--proxy',
        action='store_true',
        help='Use proxies to pass inputs',
    )
    parser.add_argument(
        '--redis-port',
        type=int,
        default=None,
        help=(
            'Optional Redis server running on the localhost '
            'to use with ProxyStore '
        ),
    )
    args = parser.parse_args()

    parsl.load()

    with contextlib.ExitStack() as stack:
        store: Store[Any] | None = None
        if args.proxy:
            connector: Connector[Any] = (
                LocalConnector()
                if args.redis_port is None
                else RedisConnector('localhost', args.redis_port)
            )
            store = stack.enter_context(Store(connector))

        mapped_results = []
        for _ in range(args.num_arrays):
            x = np.random.rand(args.size, args.size)
            if store is not None:
                x = store.proxy(x)
            mapped_results.append(app_double(x))

        total = app_sum(inputs=mapped_results)

        print('Sum:', total.result())

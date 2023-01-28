"""Peer connection transfer speed test."""
from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import time
import uuid
from typing import Sequence

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal

from proxystore.endpoint.endpoint import Endpoint
from testing.compat import randbytes


async def get_endpoint(
    actor: Literal['local', 'remote'],
    server: str,
) -> tuple[Endpoint, uuid.UUID | None]:
    """Return a ready PeerConnection."""
    endpoint = await Endpoint(
        name=socket.gethostname(),
        uuid=uuid.uuid4(),
        signaling_server=server,
    )

    print(f'Endpoint uuid: {endpoint.uuid}')
    if actor == 'local':
        remote_uuid = uuid.UUID(input('Enter the remote\'s uuid: ').strip())
    else:
        remote_uuid = None

    return endpoint, remote_uuid


async def amain(
    actor: Literal['local', 'remote'],
    size: int,
    server: str,
) -> None:
    """Measure transfer speed between producer and consumer."""
    endpoint, target_uuid = await get_endpoint(actor, server)

    if actor == 'local':
        print('Testing connection to remote')
        assert not await endpoint.exists('key', target_uuid)
        print('Connection established')

        print(f'Sending {size} bytes to remote')
        data = randbytes(size)
        start = time.perf_counter_ns()
        await endpoint.set('key', data, target_uuid)
        end = time.perf_counter_ns()
        print(f'Elapsed time: {(end - start) / 1e6:.3f} ms')
        print(f'Mbps: {(size * 8 / 1e6) / ((end - start) / 1e9)}')

        print(f'Retrieving {size} bytes to remote')
        start = time.perf_counter_ns()
        await endpoint.get('key', target_uuid)
        end = time.perf_counter_ns()
        print(f'Elapsed time: {(end - start) / 1e6:.3f} ms')
        print(f'Mbps: {(size * 8 / 1e6) / ((end - start) / 1e9)}')

        await endpoint.evict('key', target_uuid)
    elif actor == 'remote':
        print('Serving remote endpoint. Use ctrl-C twice to stop')
        # Remote endpoint should wait until interrupted so let's just sleep
        # for 10 minutes (long enough for the test)
        try:
            await asyncio.sleep(10 * 60)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass

    await endpoint.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Peer endpoint bandwidth app."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Measure transfer speed between two endpoints.',
    )
    parser.add_argument(
        'actor',
        choices=['local', 'remote'],
        help='should this process act as the local or remote endpoint',
    )
    parser.add_argument(
        '--size',
        type=int,
        help='size of data to move',
    )
    parser.add_argument(
        '--server',
        help='signaling server address',
    )
    parser.add_argument(
        '--no-uvloop',
        action='store_true',
        help='override using uvloop if available',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='set debug mode in asyncio',
    )
    args = parser.parse_args(argv)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('aiortc.rtcsctptransport').setLevel(logging.INFO)
        logging.getLogger('proxystore.p2p.connection').setLevel(logging.INFO)

    if not args.no_uvloop:
        try:
            import uvloop

            uvloop.install()
            print('using uvloop')
        except ImportError:
            print('uvloop unavailable... using default asyncio event loop')

    logging.basicConfig()

    asyncio.run(amain(args.actor, args.size, args.server), debug=args.debug)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

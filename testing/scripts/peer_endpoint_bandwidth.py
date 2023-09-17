"""Peer connection transfer speed test."""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import uuid
from typing import Literal
from typing import Sequence

from proxystore.endpoint.endpoint import Endpoint
from proxystore.p2p.manager import PeerManager
from proxystore.p2p.relay.client import RelayClient
from testing.compat import randbytes


async def get_endpoint(
    actor: Literal['local', 'remote'],
    relay_server_address: str,
) -> tuple[Endpoint, uuid.UUID | None]:
    """Return a ready PeerConnection."""
    relay_client = RelayClient(relay_server_address)
    peer_manager = await PeerManager(relay_client)
    endpoint = await Endpoint(peer_manager=peer_manager)

    print(f'Endpoint uuid: {endpoint.uuid}')
    if actor == 'local':
        remote_uuid = uuid.UUID(input("Enter the remote's uuid: ").strip())
    else:
        remote_uuid = None

    return endpoint, remote_uuid


async def amain(
    actor: Literal['local', 'remote'],
    size: int,
    relay: str,
) -> None:
    """Measure transfer speed between producer and consumer."""
    endpoint, target_uuid = await get_endpoint(actor, relay)

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
        '--relay',
        help='relay server address',
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

    asyncio.run(amain(args.actor, args.size, args.relay), debug=args.debug)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

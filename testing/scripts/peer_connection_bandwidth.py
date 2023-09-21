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

from proxystore.p2p.connection import PeerConnection
from proxystore.p2p.relay.client import RelayClient
from proxystore.p2p.relay.messages import PeerConnectionRequest
from testing.compat import randbytes


async def get_connection(
    actor: Literal['producer', 'consumer'],
    relay: str,
    channels: int = 1,
) -> PeerConnection:
    """Return a ready PeerConnection."""
    client = RelayClient(relay)
    local_uuid = client.uuid
    connection = PeerConnection(client, channels=channels)

    print(f'{actor} uuid: {local_uuid}')
    remote_uuid = uuid.UUID(input('enter the remote uuid: ').strip())

    if actor == 'producer':
        await connection.send_offer(remote_uuid)
        answer = await client.recv()
        assert isinstance(answer, PeerConnectionRequest)
        await connection.handle_server_message(answer)
    elif actor == 'consumer':
        offer = await client.recv()
        assert isinstance(offer, PeerConnectionRequest)
        await connection.handle_server_message(offer)

    await connection.ready()

    print(f'{actor} connection to remote with uuid {remote_uuid}')

    return connection


async def amain(
    actor: Literal['producer', 'consumer'],
    size: int,
    relay: str,
    channels: int = 1,
) -> None:
    """Measure transfer speed between producer and consumer."""
    connection = await get_connection(actor, relay, channels)

    data: str | bytes
    if actor == 'producer':
        data = randbytes(size)
        start = time.perf_counter()
        await connection.send(data)
        assert await connection.recv() == 'done'
        end = time.perf_counter()
    elif actor == 'consumer':
        start = time.perf_counter()
        data = await connection.recv()
        await connection.send('done')
        end = time.perf_counter()

    verb = 'transferred' if actor == 'producer' else 'received'
    data_bytes = sys.getsizeof(data)
    print(f'{verb} {data_bytes} bytes')
    print(f'time (s): {end - start:.3f}')
    print(f'mbps: {data_bytes * 8 / (end - start) / 1e6:.3f}')

    await connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Peer bandwidth app."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Measure transfer speed between two WebRTC peers.',
    )
    parser.add_argument(
        'actor',
        choices=['producer', 'consumer'],
        help='should this process produce or consume the data',
    )
    parser.add_argument(
        '--size',
        type=int,
        help='message length in bytes',
    )
    parser.add_argument(
        '--relay',
        help='relay server address',
    )
    parser.add_argument(
        '--channels',
        type=int,
        default=1,
        help='number of datachannels to split message sending over',
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

    if not args.no_uvloop:
        try:
            import uvloop

            uvloop.install()
            print('using uvloop')
        except ImportError:
            print('uvloop unavailable... using default asyncio event loop')

    logging.basicConfig()

    asyncio.run(
        amain(args.actor, args.size, args.relay, args.channels),
        debug=args.debug,
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

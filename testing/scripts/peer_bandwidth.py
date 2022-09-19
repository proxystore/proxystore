"""Peer transfer speed test."""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import uuid
from typing import Literal
from typing import Sequence

from proxystore.p2p import messages
from proxystore.p2p.client import connect
from proxystore.p2p.connection import PeerConnection


async def get_connection(
    actor: Literal['producer', 'consumer'],
    server: str,
) -> PeerConnection:
    """Returns a ready PeerConnection."""
    local_uuid, name, websocket = await connect(server)
    connection = PeerConnection(local_uuid, name, websocket)

    print(f'{actor} uuid: {local_uuid}')
    remote_uuid = uuid.UUID(input('enter the remote uuid: ').strip())

    if actor == 'producer':
        await connection.send_offer(remote_uuid)
        answer = messages.decode(await websocket.recv())  # type: ignore
        await connection.handle_server_message(answer)  # type: ignore
    elif actor == 'consumer':
        offer = messages.decode(await websocket.recv())  # type: ignore
        await connection.handle_server_message(offer)  # type: ignore

    await connection.ready()

    print(f'{actor} connection to remote with uuid {remote_uuid}')

    return connection


async def amain(
    actor: Literal['producer', 'consumer'],
    size: int,
    server: str,
) -> None:
    """Measure transfer speed between producer and consumer."""
    connection = await get_connection(actor, server)

    if actor == 'producer':
        data = 'x' * size
        start = time.perf_counter()
        await connection.send(data)
        assert await connection.recv() == 'done'
        end = time.perf_counter()
    elif actor == 'consumer':
        start = time.perf_counter()
        data = await connection.recv()
        await connection.send('done')
        end = time.perf_counter()

    print('buf', connection._channel.bufferedAmountLowThreshold)

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
        description='Measure transfer speed between two RTC Peers.',
    )
    parser.add_argument(
        'actor',
        choices=['producer', 'consumer'],
        help='should this process produce or consume the data',
    )
    parser.add_argument(
        '--size',
        type=int,
        help='message length in characters',
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
    args = parser.parse_args(argv)

    if not args.no_uvloop:
        try:
            import uvloop

            uvloop.install()
            print('using uvloop')
        except ImportError:
            print('uvloop unavailable... using default asyncio event loop')

    logging.basicConfig()

    asyncio.run(amain(args.actor, args.size, args.server))

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

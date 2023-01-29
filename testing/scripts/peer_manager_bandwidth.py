"""Peer connection transfer speed test."""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import uuid
from typing import Sequence

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal

from proxystore.p2p.manager import PeerManager
from testing.compat import randbytes


async def get_manager(
    actor: Literal['producer', 'consumer'],
    server: str,
) -> tuple[PeerManager, uuid.UUID]:
    """Return a ready PeerManager."""
    manager = await PeerManager(uuid.uuid4(), server)

    print(f'{actor} uuid: {manager.uuid}')
    remote_uuid = uuid.UUID(input('enter the remote uuid: ').strip())

    if actor == 'producer':
        await manager.send(remote_uuid, 'hello')
    elif actor == 'consumer':
        _, message = await manager.recv()
        assert message == 'hello'

    connection = await manager.get_connection(remote_uuid)
    await connection.ready()

    print(f'{actor} connection to remote with uuid {remote_uuid}')

    return manager, remote_uuid


async def amain(
    actor: Literal['producer', 'consumer'],
    size: int,
    server: str,
) -> None:
    """Measure transfer speed between producer and consumer."""
    manager, remote_uuid = await get_manager(actor, server)

    data: str | bytes
    if actor == 'producer':
        data = randbytes(size)
        start = time.perf_counter()
        await manager.send(remote_uuid, data)
        _, message = await manager.recv()
        assert message == 'done'
        end = time.perf_counter()
    elif actor == 'consumer':
        start = time.perf_counter()
        _, data = await manager.recv()
        await manager.send(remote_uuid, 'done')
        end = time.perf_counter()

    verb = 'transferred' if actor == 'producer' else 'received'
    data_bytes = sys.getsizeof(data)
    print(f'{verb} {data_bytes} bytes')
    print(f'time (s): {end - start:.3f}')
    print(f'mbps: {data_bytes * 8 / (end - start) / 1e6:.3f}')

    await manager.close()


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

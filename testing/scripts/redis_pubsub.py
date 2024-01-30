"""Redis pub/sub test.

1. Start a Redis server.

   $ redis-server --protected-mode no --save "" --appendonly no

2. Start the subscriber.

   $ python testing/scripts/redis_pubsub.py subscriber

3. Start the publisher.
C
   $ python testing/scripts/redis_pubsub.py publisher
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Sequence

from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber

MESSAGES = [f'message_{i}'.encode() for i in range(10)]


def publish(host: str, port: int, delay: float) -> None:
    """Publish messages to the stream."""
    publisher = RedisPublisher(host, port)

    for message in MESSAGES:
        publisher.send('default', message)
        print(f'Sent: {message!r}')
        time.sleep(delay)

    publisher.close()
    print('Publisher closed')


def subscribe(host: str, port: int) -> None:
    """Subscribe to messages from the stream."""
    subscriber = RedisSubscriber(host, port, 'default')

    print('Listening for messages...')

    for message in subscriber:
        print(f'Received: {message!r}')

    print('Publisher closed topic')

    subscriber.close()
    print('Subscriber closed')


def main(argv: Sequence[str] | None = None) -> int:
    """Redis pub/sub test."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Test the Redis pub/sub interface with a Redis server',
    )
    parser.add_argument(
        'role',
        choices=['publisher', 'subscriber'],
        help='Role to perform',
    )
    parser.add_argument(
        '--host',
        default='localhost',
        help='Redis server host',
    )
    parser.add_argument(
        '--port',
        default=6379,
        type=int,
        help='Redis server port',
    )
    parser.add_argument(
        '--delay',
        default=1.0,
        type=float,
        help='Delay in seconds between sending messages',
    )
    args = parser.parse_args(argv)

    if args.role == 'publisher':
        publish(args.host, args.port, args.delay)
    elif args.role == 'subscriber':
        subscribe(args.host, args.port)
    else:
        raise AssertionError(f'Unknown role type "{args.role}"')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

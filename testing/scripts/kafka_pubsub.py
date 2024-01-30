"""Kafka pub/sub test.

1. Start a Kafka broker. This varies, but I found this docker compose file
   to be useful: https://sahansera.dev/setting-up-kafka-locally-for-testing/

2. Start the subscriber.

   $ python testing/scripts/kafka_pubsub.py subscriber --broker localhost:9092

3. Start the publisher.
C
   $ python testing/scripts/kafka_pubsub.py publisher --broker localhost:9092
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Sequence

import kafka

from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber

MESSAGES = [f'message_{i}'.encode() for i in range(10)]


def publish(broker: str, delay: float) -> None:
    """Publish messages to the stream."""
    producer = kafka.KafkaProducer(bootstrap_servers=[broker])
    publisher = KafkaPublisher(producer)

    for message in MESSAGES:
        publisher.send('default', message)
        print(f'Sent: {message!r}')
        time.sleep(delay)

    publisher.close()
    print('Publisher closed')


def subscribe(broker: str) -> None:
    """Subscribe to messages from the stream."""
    consumer = kafka.KafkaConsumer('default', bootstrap_servers=[broker])
    subscriber = KafkaSubscriber(consumer)

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
        '--broker',
        default='localhost:9092',
        help='Kafka broker address',
    )
    parser.add_argument(
        '--delay',
        default=1.0,
        type=float,
        help='Delay in seconds between sending messages',
    )
    args = parser.parse_args(argv)

    if args.role == 'publisher':
        publish(args.broker, args.delay)
    elif args.role == 'subscriber':
        subscribe(args.broker)
    else:
        raise AssertionError(f'Unknown role type "{args.role}"')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

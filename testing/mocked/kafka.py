from __future__ import annotations

import queue
import sys
from typing import NamedTuple

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

import confluent_kafka


class MockMessage(NamedTuple):
    topic: str
    message: bytes
    exception: confluent_kafka.KafkaError | None

    def error(self) -> confluent_kafka.KafkaError | None:
        return self.exception

    def value(self) -> bytes:
        return self.message


class MockKafkaProducer(confluent_kafka.Producer):
    def __init__(self, queues: dict[str, queue.Queue[MockMessage]]) -> None:
        self._queues = queues

    def flush(self, timeout: float | None = None) -> None:
        pass

    def produce(self, topic: str, data: bytes) -> None:
        message = MockMessage(topic, data, None)
        self._queues[topic].put(message)


class MockKafkaConsumer(confluent_kafka.Consumer):
    def __init__(self, queue_: queue.Queue[MockMessage]) -> None:
        self._queue = queue_

    def poll(self, timeout: float | None = None) -> MockMessage:
        return self._queue.get()

    def close(self) -> None:
        pass


def make_producer_consumer_pair(
    topic: str = 'default',
) -> tuple[MockKafkaProducer, MockKafkaConsumer]:
    queue_: queue.Queue[MockMessage] = queue.Queue()
    return MockKafkaProducer({topic: queue_}), MockKafkaConsumer(queue_)

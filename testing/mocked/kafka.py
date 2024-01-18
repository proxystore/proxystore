from __future__ import annotations

import queue
import sys
from typing import NamedTuple

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import kafka


class Message(NamedTuple):
    topic: str
    value: bytes


class Future:
    def get(self) -> None:
        pass


class MockKafkaProducer(kafka.KafkaProducer):
    def __init__(self, queues: dict[str, queue.Queue[Message]]) -> None:
        self._queues = queues

    def close(self) -> None:
        pass

    def send(self, topic: str, data: bytes) -> Future:
        message = Message(topic, data)
        self._queues[topic].put(message)
        return Future()


class MockKafkaConsumer(kafka.KafkaConsumer):
    def __init__(self, queue_: queue.Queue[Message]) -> None:
        self._queue = queue_

    def __iter__(self) -> Self:  # pragma: no cover
        return self

    def __next__(self) -> Message:
        return self._queue.get()

    def close(self) -> None:
        pass


def make_producer_consumer_pair(
    topic: str = 'default',
) -> tuple[MockKafkaProducer, MockKafkaConsumer]:
    queue_: queue.Queue[Message] = queue.Queue()
    return MockKafkaProducer({topic: queue_}), MockKafkaConsumer(queue_)

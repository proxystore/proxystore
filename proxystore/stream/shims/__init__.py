"""Shim interfaces to common message brokers.

The [`Publisher`][proxystore.stream.protocols.Publisher] and
[`Subscriber`][proxystore.stream.protocols.Subscriber] are
[`Protocols`][typing.Protocol] which define the publisher and subscriber
interfaces to a pub/sub-like message broker.

This sub-package provides a set of shim or adapter interfaces to
common pub/sub systems like Kafka, Redis, and ZeroMQ. Generally, these
shims are very lightweight and mostly serve to adapt the third-party
interface to match the [`Publisher`][proxystore.stream.protocols.Publisher] and
[`Subscriber`][proxystore.stream.protocols.Subscriber] protocols expected
by the [StreamProducer][proxystore.stream.interface.StreamProducer]
and [StreamConsumer][proxystore.stream.interface.StreamConsumer].

Warning:
    Most of the provided shims have a external dependency that may not be
    installed by default with ProxyStore.
"""

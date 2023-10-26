![ProxyStore Schematic](../static/proxystore-schematic.svg){ width="75%" style="display: block; margin: 0 auto" }
> **Figure 1:** High-level overview of how the ProxyStore components fit together.

ProxyStore is composed of three main components: the
[`Proxy`][proxystore.proxy.Proxy],
[`Connector`][proxystore.connectors.protocols.Connector],
and [`Store`][proxystore.store.base.Store].

The [`Proxy`][proxystore.proxy.Proxy] model provides *pass-by-reference*
semantics and *just-in-time* object resolution transparently to consumers.

The [`Connector`][proxystore.connectors.protocols.Connector] is a
[`Protocol`][typing.Protocol] that defines the low-level
interface to a mediated communication channel or object store.
Many [`Connector`][proxystore.connectors.protocols.Connector] implementations
are provided in the [`proxystore.connectors`][proxystore.connectors] module,
and users can easily create their own.

The [`Store`][proxystore.store.base.Store] is a high-level abstraction of an
object store and the intended means by which an application uses ProxyStore.
The [`Store`][proxystore.store.base.Store] is initialized with
a [`Connector`][proxystore.connectors.protocols.Connector] and provides
extra functionality like caching and serialization. Most important is that the
[`.proxy()`][proxystore.store.base.Store.proxy] method is provided which can
produce a [`Proxy`][proxystore.proxy.Proxy] of an arbitrary object put in the
store.

Continue reading to learn more about these concepts.

* [Proxy](proxy.md)
* [Connector](connector.md)
* [Store](store.md)

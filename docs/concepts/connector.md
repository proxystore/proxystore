The [`Connector`][proxystore.connectors.protocols.Connector] is a
[`Protocol`][typing.Protocol] that defines the low-level
interface to a mediated communication channel or object store.
The [`Connector`][proxystore.connectors.protocols.Connector] methods operate
on bytes-like data and keys which are tuples of metadata that can
identify a unique object.

The protocol is as follows:
```python title="Connector Protocol" linenums="1"
KeyT = TypeVar('KeyT', bound=NamedTuple)


class Connector(Protocol[KeyT]):
    def close(self, *, clear: bool | None = None) -> bool | None: ...
    def config(self) -> dict[str, Any]: ...
    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self: ...
    def evict(self, key: KeyT) -> None: ...
    def exists(self, key: KeyT) -> bool: ...
    def get(self, key: KeyT) -> BytesLike | None: ...
    def get_batch(self, keys: Sequence[KeyT]) -> list[BytesLike | None]: ...
    def put(self, obj: BytesLike) -> KeyT: ...
    def put_batch(self, objs: Sequence[BytesLike]) -> list[KeyT]: ...
```

## Implementations

Implementing a custom [`Connector`][proxystore.connectors.protocols.Connector]
requires creating a class which implements the above methods. Note that
the custom class does not need to inherit from
[`Connector`][proxystore.connectors.protocols.Connector] because it is a
[`Protocol`][typing.Protocol].

Many [`Connector`][proxystore.connectors.protocols.Connector] implementations
are provided in the [`proxystore.connectors`][proxystore.connectors] module,
and users can easily create their own.
A [`Connector`][proxystore.connectors.protocols.Connector] instance is used
by the [`Store`][proxystore.store.base.Store] to store and retrieve serialized objects.

The `clear` argument of `close()` controls whether the objects stored by the connector are also removed (e.g., deleting the directory used by the [`FileConnector`][proxystore.connectors.file.FileConnector]).
When `clear` is `None`, the connector should use its own default.
`close()` should return `True` if the objects were removed, so the owner [`Store`][proxystore.store.base.Store] knows its proxies can no longer be resolved in this process.
A [`Store`][proxystore.store.base.Store] only clears its connector when closed if it is the [`owner`][proxystore.store.base.Store.owner]; stores created implicitly, such as when a proxy is resolved in another process, always pass `#!python clear=False`.

Connectors must be thread-safe.
A [`Store`][proxystore.store.base.Store] may call the methods of one connector from multiple threads at the same time, including with the same key (e.g., two threads evicting a key while another gets it).
If a connector uses a client which is not thread-safe, guard the client with a lock.

Implementations of `put()` and `put_batch()` may accept additional, connector-specific keyword arguments.
These are passed from [`Store`][proxystore.store.base.Store] methods via the `connector_options` parameter.
For example, the [`MultiConnector`][proxystore.connectors.multi.MultiConnector] routes objects based on tags.

```python
store.put(obj, connector_options={'subset_tags': ['small']})
```

## Extensions

A [`Connector`][proxystore.connectors.protocols.Connector] implementation
can be extended to implement the
[`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
protocol. A
[`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
provides methods for creating a key and then setting that key to an object
at a later time. Not all of the provided
[`Connector`][proxystore.connectors.protocols.Connector] implementations
implement the
[`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
protocol because some transfer methods require the object before creating a
key for that object.

## Security

A [`Store`][proxystore.store.base.Store] deserializes the bytes a connector returns with pickle by default, so anyone who can write to a connector's storage can run code in the processes that read from it (see [Store Security](store.md#security)).
Connectors differ in who can read and write their objects.

* [`LocalConnector`][proxystore.connectors.local.LocalConnector]: only the current process.
* [`FileConnector`][proxystore.connectors.file.FileConnector]: anyone with access to the directory.
  The directory is created with the default permissions of your [umask](https://en.wikipedia.org/wiki/Umask){target=_blank}, which often lets other users read it, so use a directory that only you can access on shared systems.
* [`RedisConnector`][proxystore.connectors.redis.RedisConnector]: anyone who can connect to the Redis server.
  The connector does not use a password or TLS, so only use it with a Redis server on a trusted network.
* [`ZeroMQConnector`][proxystore.connectors.zmq.ZeroMQConnector]: anyone who can reach the server's port.
  The servers do not authenticate clients or encrypt data, so only use this connector within a trusted network, such as the interconnect of an HPC cluster.
* [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector]: processes that can read the endpoint directory, and peer endpoints you allow.
  See [Endpoint Security](../guides/endpoints.md#security).
* [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector]: anyone with access to the directories on the Globus collections, as set by the file systems and the permissions of each collection.
* [`DAOSConnector`][proxystore.connectors.daos.DAOSConnector]: anyone with access to the DAOS container, as set by the DAOS pool and container access control lists.
* [`MultiConnector`][proxystore.connectors.multi.MultiConnector]: anyone who can access any of its connectors.

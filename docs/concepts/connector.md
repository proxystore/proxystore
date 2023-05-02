The [`Connector`][proxystore.connectors.connector.Connector] is a
[`Protocol`][typing.Protocol] that defines the low-level
interface to a mediated communication channel or object store.
The [`Connector`][proxystore.connectors.connector.Connector] methods operate
of [`bytes`][bytes] of data and keys which are tuples of metadata that can
identify a unique object.

The protocol is as follows:
```python title="Connector Protocol" linenums="1"
KeyT = TypeVar('KeyT', bound=NamedTuple)

class Connector(Protocol[KeyT]):
    def close(self) -> None: ...
    def config(self) -> dict[str, Any]: ...
    def from_config(self, config: dict[str, Any]) -> Connector[KeyT]: ...
    def evict(self, key: KeyT) -> None: ...
    def exists(self, key: KeyT) -> bool: ...
    def get(self, key: KeyT) -> bytes | None: ...
    def get_batch(self, Sequence[KeyT]) -> list[bytes | None]: ...
    def put(self, obj: bytes) -> KeyT: ...
    def put_batch(self, objs: Sequence[bytes]) -> list[KeyT]: ...
```
Implementing a custom [`Connector`][proxystore.connectors.connector.Connector]
requires creating a class which implements the above methods. Note that
the custom class does not need to inherit from
[`Connector`][proxystore.connectors.connector.Connector] because it is a
[`Protocol`][typing.Protocol].

Many [`Connector`][proxystore.connectors.connector.Connector] implementations
are provided in the [`proxystore.connectors`][proxystore.connectors] module,
and users can easily create their own.
A [`Connector`][proxystore.connectors.connector.Connector] instance is used
by the [`Store`][proxystore.store.base.Store] to interact with the store.

# Upgrading to ProxyStore v2

ProxyStore v2 makes stores safe to share between applications; promotes
streaming, reference ownership, and proxy futures to stable features;
rebuilds [ProxyStore Endpoints](guides/endpoints.md) with more reliable
peer-to-peer communication, authentication, and encryption; and makes the
base install lighter. This guide walks through the changes to make when
upgrading from v1.x.

## TL;DR

Most applications only need the following changes. The rest of this guide
has the details.

* Upgrade every process which shares proxies at the same time, because
  proxies are not compatible between v1 and v2
  ([Compatibility](#compatibility)).
* Stop endpoints **before** upgrading, then configure them again
  ([Endpoints](#endpoints)).
* Use Python 3.11 or newer, and install `proxystore[globus]` if you use
  Globus ([Installation](#installation)).
* Create stores with `Store(connector)`. Names are optional labels, and
  stores register themselves, so remove `register=True` and look-ups by name
  ([Constructor and Names](#constructor-and-names)).
* Pass serializers to the `Store` constructor instead of to each method
  ([One Serializer per Store](#one-serializer-per-store)), and pass
  connector-specific options through `connector_options`
  ([Connector Options](#connector-options)).
* Replace `cache_size=` with `cache_size_bytes=`
  ([Cache Size in Bytes](#cache-size-in-bytes)).
* Rename `filter_=` to `keep=` in streams and invert your filters
  ([Stream Filters](#stream-filters)).
* Replace `Future` with `ProxyFuture` and the `polling_*` arguments with a
  `PollingPolicy` ([Proxy Futures](#proxy-futures)).

## Compatibility

!!! warning "Proxies are not compatible between v1 and v2"

    Proxies, store configs, and stream events created by v1 cannot be used
    by v2, and vice versa. Upgrade every process that creates or resolves
    proxies (clients, workers, and endpoints) at the same time, and discard
    any pickled proxies or stream events persisted by v1.

Starting with v2, ProxyStore commits to compatibility within a major
version. A proxy, store config, or stream event created by any 2.x release
can be used by any other 2.x release, in both directions, so future
upgrades within 2.x do not need to happen everywhere at once. See
[Versioning and Compatibility](versioning.md) for the full policy, including
which modules are public API.

## Installation

* **Python 3.11+** is required.
* **`proxystore-ex` is now part of ProxyStore.** The
  [`DAOSConnector`][proxystore.connectors.daos.DAOSConnector] and
  [`ZeroMQConnector`][proxystore.connectors.zmq.ZeroMQConnector] moved into
  ProxyStore (see [Moved from `proxystore-ex`](#moved-from-proxystore-ex)),
  and the `extensions` extra and `proxystore.ex` module were removed. Remove
  `proxystore-ex` from your dependencies.
* **The base install is lighter.** Optional dependencies moved to extras.
  Notably, install `proxystore[globus]` if you use the
  [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector] or the
  `proxystore-globus-auth` CLI. Importing a feature without its extra raises
  an [`ImportError`][ImportError] which says what to install.

## Stores

### Constructor and Names

Stores are now identified by a unique, randomly generated ID rather than a
user-chosen name. In v1, two stores with the same name but different
connectors (e.g., from two applications sharing a Dask or Globus Compute
worker) collided in the per-process registry, and proxies could silently
resolve through the wrong connector. The name is now an optional label used
only in logs.

The connector is the first positional argument of
[`Store`][proxystore.store.base.Store], and `name` is optional and
keyword-only.

```python
# v1
store = Store('my-store', FileConnector('/tmp/data'), register=True)

# v2
store = Store(FileConnector('/tmp/data'))
store = Store(FileConnector('/tmp/data'), name='my-store')  # Optional label
```

Every `Store` registers itself when created and unregisters itself when
closed, so `register=True` is no longer needed (or accepted). We recommend
using stores as context managers.

```python
with Store(FileConnector('/tmp/data')) as store:
    proxy = store.proxy(obj)
```

### Removed Registration API

| Removed | Replacement |
| --- | --- |
| `Store(..., register=True)` | None. Stores are always registered. |
| `StoreConfig.auto_register` | None. Remove the field from configs and TOML files (unknown fields fail validation). |
| `get_store('name')` | Keep a reference to the `Store`, or pass [`store.config()`][proxystore.store.base.Store.config] to other processes and use [`Store.from_config()`][proxystore.store.base.Store.from_config] or [`get_or_create_store()`][proxystore.store.get_or_create_store]. |
| `register_store()`, `unregister_store()`, `store_registration()` | None. Stores register when created and unregister on [`close()`][proxystore.store.base.Store.close]. |
| `StoreExistsError` | None. It is no longer raised. |

[`get_store(proxy)`][proxystore.store.get_store] and
[`get_or_create_store(config)`][proxystore.store.get_or_create_store] remain,
with two changes:

* `get_store(proxy)` always returns a store (creating it from the proxy's
  config if needed) rather than `None`.
* `get_or_create_store()` no longer takes `register=`.

### Ownership and Clearing on Close

In v1, connectors that clear their data on close (e.g.,
[`FileConnector`][proxystore.connectors.file.FileConnector] and
[`GlobusConnector`][proxystore.connectors.globus.GlobusConnector], which
default to `clear=True`) copied that setting into their config. As a result,
*any* store created from that config deleted the shared data when closed.
This happened, for example, in
[`StreamConsumer.close(stores=True)`][proxystore.stream.StreamConsumer.close],
lifetimes with `close_stores=True`,
[`StoreExecutor.shutdown()`][proxystore.store.executor.StoreExecutor.shutdown],
or a worker running `with Store.from_config(config)`.

In v2, stores have an `owner` flag, and only the owner clears its connector
when closed.

* `Store(...)` is the owner by default (`owner=True`).
* Stores created implicitly are not owners. This covers stores created when
  resolving a proxy, by `get_or_create_store()`, by stream consumers, by
  lifetimes, and by `StoreExecutor` workers.
* [`Store.from_config(config)`][proxystore.store.base.Store.from_config]
  creates an owner only if the config has no ID (e.g., a config loaded from
  a TOML file). A config with an ID came from an existing store, which is
  the owner. Pass `owner=True` or `owner=False` to choose.
* [`Store.close(*, clear=None)`][proxystore.store.base.Store.close] replaces
  `Store.close(*args, **kwargs)`. With `clear=None`, an owner uses the
  connector's default and other stores never clear. Pass `clear=True` or
  `clear=False` to override.

```python
# v1: forwarded to the connector's close()
store.close(clear=False)

# v2: clears only if this store is the owner and the connector clears by
# default. Pass clear=False to never clear.
store.close()
```

### Connector Options

[`Store`][proxystore.store.base.Store] methods no longer forward arbitrary
`**kwargs` to the connector. Pass connector-specific options (e.g.,
[`MultiConnector`][proxystore.connectors.multi.MultiConnector] tags) through
`connector_options`.

```python
# v1
store.put(obj, subset_tags=['local'])
store.proxy(obj, subset_tags=['local'])

# v2
store.put(obj, connector_options={'subset_tags': ['local']})
store.proxy(obj, connector_options={'subset_tags': ['local']})
```

### One Serializer per Store

[`Store`][proxystore.store.base.Store] methods no longer take `serializer`
or `deserializer` arguments. Every operation of a store, and every proxy it
creates, uses the serializer and deserializer passed to the `Store`
constructor.

```python
# v1
store.put(data, serializer=my_serialize)
obj = store.get(key, deserializer=my_deserialize)

# v2
store = Store(connector, serializer=my_serialize, deserializer=my_deserialize)
store.put(data)
obj = store.get(key)
```

The arguments were removed from
[`put()`][proxystore.store.base.Store.put],
[`put_batch()`][proxystore.store.base.Store.put_batch],
[`get()`][proxystore.store.base.Store.get],
[`get_batch()`][proxystore.store.base.Store.get_batch],
[`proxy()`][proxystore.store.base.Store.proxy],
[`proxy_batch()`][proxystore.store.base.Store.proxy_batch],
[`proxy_from_key()`][proxystore.store.base.Store.proxy_from_key],
[`locked_proxy()`][proxystore.store.base.Store.locked_proxy],
[`owned_proxy()`][proxystore.store.base.Store.owned_proxy], and
[`future()`][proxystore.store.base.Store.future], as well as from
[`StoreFactory`][proxystore.store.factory.StoreFactory],
[`PollingStoreFactory`][proxystore.store.factory.PollingStoreFactory], and
[`update()`][proxystore.store.ref.update]. Proxies also no longer carry a
deserializer.

* To store some types in a custom format, handle those types in the store's
  serializer and fall back to
  [`serialize()`][proxystore.serialize.serialize] for the rest (see
  [Store](concepts/store.md) for an example).
* To store data which is already serialized, use a store whose serializer
  passes bytes through and set `populate_target=False`.

### Cache Size in Bytes

`Store(cache_size=16)`, a limit on the number of cached objects, is replaced
by `Store(cache_size_bytes=...)`, a limit on their total size. The default is
256 MiB per store in each process. Objects larger than the limit are not
cached. Passing `cache_size=` raises a [`TypeError`][TypeError], and a TOML
config with `cache_size` fails to load.

Each cache entry counts as the size of the object's serialized data, so with
the default `cache_mode='objects'` the limit is an estimate of the memory
used.

### Non-Proxiable Objects

[`Store.proxy()`][proxystore.store.base.Store.proxy] and
[`Store.proxy_batch()`][proxystore.store.base.Store.proxy_batch] now default
to `skip_nonproxiable=True`, matching
[`locked_proxy()`][proxystore.store.base.Store.locked_proxy] and
[`owned_proxy()`][proxystore.store.base.Store.owned_proxy]. Objects that
cannot be proxied (e.g., `bool` and `None`) are returned as-is rather than
raising
[`NonProxiableTypeError`][proxystore.store.exceptions.NonProxiableTypeError].
Pass `skip_nonproxiable=False` to keep the v1 behavior.

### Other Store Changes

* **Immutable configs:**
  [`StoreConfig`][proxystore.store.config.StoreConfig] and
  [`ConnectorConfig`][proxystore.store.config.ConnectorConfig] are frozen,
  so setting a field raises a `ValidationError`. Use
  `config.model_copy(update={...})` to make a changed copy.
  [`Store.config()`][proxystore.store.base.Store.config] is made when the
  store is created and returns the same object each time.
* **Read-only attributes:**
  [`Store.connector`][proxystore.store.base.Store.connector] and
  [`Store.cache`][proxystore.store.base.Store.cache] are read-only
  properties, so a store's connector cannot be replaced after it is created.
* **Exceptions:**
  [`ProxyResolveMissingKeyError`][proxystore.store.exceptions.ProxyResolveMissingKeyError]
  and
  [`NonProxiableTypeError`][proxystore.store.exceptions.NonProxiableTypeError]
  now subclass [`StoreError`][proxystore.store.exceptions.StoreError]. If
  you catch `StoreError` before these, check that your handlers still behave
  as intended.
* **Metrics:** Factory resolve times in
  [`StoreMetrics`][proxystore.store.metrics.StoreMetrics] were recorded in
  nanoseconds in v1 despite being documented as milliseconds. They are now
  in milliseconds.
* **New `Store.get_batch()`:**
  [`Store.get_batch()`][proxystore.store.base.Store.get_batch] returns
  objects from the cache and fetches the rest in a single
  [`Connector.get_batch()`][proxystore.connectors.protocols.Connector.get_batch]
  call.

## Connectors

### Moved from `proxystore-ex`

| v1 | v2 |
| --- | --- |
| `proxystore.ex.connectors.daos.DAOSConnector` | [`proxystore.connectors.daos.DAOSConnector`][proxystore.connectors.daos.DAOSConnector] |
| `proxystore.ex.connectors.dim.zmq.ZeroMQConnector` | [`proxystore.connectors.zmq.ZeroMQConnector`][proxystore.connectors.zmq.ZeroMQConnector] (install the `zmq` extra) |

`proxystore.connectors.dim`, which only contained a deprecation notice, was
removed. The other connectors in `proxystore-ex` were not ported.

`ZeroMQConnector.close(kill_server=...)` is replaced by the standard
[`close(clear=...)`][proxystore.connectors.zmq.ZeroMQConnector.close] and a
`clear` constructor option. Config shortcuts like
`ConnectorConfig(kind='zmq')` and `kind='daos'` now work.

### Behavior Changes

* [`FileConnector`][proxystore.connectors.file.FileConnector] creates
  `store_dir` and its files so only you can access them (`dir_mode=0o700`,
  `file_mode=0o600`), regardless of your umask. Pass
  `dir_mode=0o770, file_mode=0o660` to share objects with a group. An
  existing `store_dir` keeps its permissions.
* `FileConnector` only removes `store_dir` when cleared if it created the
  directory. A `store_dir` which already existed, and the objects in it, is
  left as is and a warning is logged.
* [`EndpointConnector.close(clear=True)`][proxystore.connectors.endpoint.EndpointConnector.close]
  logs a warning because endpoints never remove objects when a connector is
  closed.
* `FileConnector` writes each object to a hidden temporary file and renames
  it, instead of writing a `.ready` marker file next to each object, so v2
  cannot read a directory written by v1.
* Stores using a
  [`MultiConnector`][proxystore.connectors.multi.MultiConnector] can now be
  created from their config, so proxies of objects in a `MultiConnector`
  resolve in other processes.
* `FileConnector(store_dir=...)`, `EndpointConnector(proxystore_dir=...)`,
  and [`GlobusEndpoint(local_path=...)`][proxystore.connectors.globus.GlobusEndpoint]
  accept [`os.PathLike`][os.PathLike] in addition to `str`.
* The `EndpointConnector` changed substantially (see
  [`EndpointConnector`](#endpointconnector)).

### Custom Connectors

If you implement your own connector, update it to the v2
[`Connector`][proxystore.connectors.protocols.Connector] protocol:

* [`close()`][proxystore.connectors.protocols.Connector.close] must accept a
  keyword-only `clear` argument:
  `def close(self, *, clear: bool | None = None) -> None`. `None` means use
  the connector's default. Connectors without persistent state can ignore it.
* [`from_config()`][proxystore.connectors.protocols.Connector.from_config]
  should return `Self`. `Store.from_config()` now calls
  `Connector.from_config()` instead of passing the options to the
  constructor, so this method is now used.
* Connectors must be thread-safe. In v1, `Store` only called its connector
  from one thread at a time. In v2, it may call one connector from multiple
  threads at the same time, including with the same key. If your connector
  uses a client which is not thread-safe, guard the client with a lock.
* Options passed to `Store` methods via `connector_options` are forwarded to
  [`put()`][proxystore.connectors.protocols.Connector.put] and
  [`put_batch()`][proxystore.connectors.protocols.Connector.put_batch] as
  keyword arguments.
* [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector]
  now extends `Connector`, and
  [`new_key()`][proxystore.connectors.protocols.DeferrableConnector.new_key]
  accepts any `BytesLike` (not only `bytes`).

## Proxies

**`proxystore.factory` was removed.** `Factory`, `SimpleFactory`, and
`LambdaFactory` were unused by ProxyStore.
[`Proxy`][proxystore.proxy.Proxy] accepts any callable as its factory, such
as a [`functools.partial`][functools.partial].

```python
# v1
from proxystore.factory import LambdaFactory

proxy = Proxy(LambdaFactory(load, path))

# v2
import functools

proxy = Proxy(functools.partial(load, path))
```

Other changes which need no action:

* Proxies now support [`round(proxy, ndigits)`][round],
  [`math.trunc()`][math.trunc], [`math.floor()`][math.floor], and
  [`math.ceil()`][math.ceil]. In v1, `math.floor()` and `math.ceil()`
  converted the target to a `float` and lost precision for large ints.
* Deserialization reads data in place, avoiding a copy of the received
  buffer.
* [`proxystore.serialize`][proxystore.serialize] no longer imports `numpy`,
  `pandas`, or `polars` eagerly, which reduces its import time from ~230 ms
  to ~30 ms.

## Streaming

Streaming ([`proxystore.stream`][proxystore.stream]), reference ownership,
and proxy futures are now stable. Importing `proxystore.stream` no longer
emits an [`ExperimentalWarning`][proxystore.warnings.ExperimentalWarning],
so you can remove any warning filters for it.

### Stream Filters

Filters now return `True` to **keep** an event, like Python's builtin
[`filter()`][filter]. In v1, `True` meant *drop*. The parameter was renamed
from `filter_` to `keep` so code that is not updated fails loudly instead of
silently inverting.

```python
# v1: drop events whose metadata has `skip=True`
consumer = StreamConsumer(subscriber, filter_=lambda m: m.get('skip', False))

# v2: keep events whose metadata does not have `skip=True`
consumer = StreamConsumer(subscriber, keep=lambda m: not m.get('skip', False))
```

[`SamplingFilter(p)`][proxystore.stream.filters.SamplingFilter] now keeps
each event with probability `p` (in v1, `p` was the probability of
dropping). Replace `SamplingFilter(p)` with `SamplingFilter(1 - p)` to keep
the v1 behavior.

### Stream Fixes

* [`StreamConsumer`][proxystore.stream.StreamConsumer] caches stores by
  store ID instead of by topic. In v1, all events in a topic were resolved
  with the first store seen for that topic, so events from producers using
  different stores failed to resolve.
* Streaming `None` with a store now works instead of raising
  [`ValueError`][ValueError].
* A closed topic sends one end-of-stream event rather than one per flush.

## Proxy Futures

`Future` was renamed to
[`ProxyFuture`][proxystore.store.future.ProxyFuture] to avoid confusion with
[`concurrent.futures.Future`][concurrent.futures.Future], and
[`Store.future()`][proxystore.store.base.Store.future] takes a
[`PollingPolicy`][proxystore.store.future.PollingPolicy] instead of four
`polling_*` arguments.

```python
# v1
from proxystore.store.future import Future

future = store.future(polling_interval=0.1, polling_timeout=10)

# v2
from proxystore.store.future import PollingPolicy, ProxyFuture

future = store.future(polling=PollingPolicy(interval=0.1, timeout=10))
```

| v1 argument | `PollingPolicy` field |
| --- | --- |
| `polling_interval` | `interval` |
| `polling_backoff_factor` | `backoff_factor` |
| `polling_interval_limit` | `interval_limit` |
| `polling_timeout` | `timeout` |

`ProxyFuture` also adds:

* [`result(timeout=...)`][proxystore.store.future.ProxyFuture.result],
  which raises [`TimeoutError`][TimeoutError].
* [`set_exception()`][proxystore.store.future.ProxyFuture.set_exception],
  which passes a producer's failure to `result()` and to the future's proxy.
  The exception is stored as the result of the future using the store's
  serializer, so any result which is an exception is raised the same way.

## Reference Ownership

These changes affect [`proxystore.store.ref`][proxystore.store.ref],
[`proxystore.store.scopes`][proxystore.store.scopes], and the
[`StoreExecutor`][proxystore.store.executor.StoreExecutor].

* **Pickling an `OwnedProxy` more than once raises an error.** Pickling an
  [`OwnedProxy`][proxystore.store.ref.OwnedProxy] that was already pickled
  raises
  [`ReferenceInvalidError`][proxystore.store.ref.ReferenceInvalidError], and
  pickling one that is borrowed raises
  [`ReferenceBorrowedError`][proxystore.store.ref.ReferenceBorrowedError].
  In v1, both worked but made two owners or left borrows pointing at an
  object the new owner could evict. Pass a
  [`borrow()`][proxystore.store.ref.borrow], a
  [`clone()`][proxystore.store.ref.clone], or a plain `Proxy` to frameworks
  that pickle arguments more than once (e.g., to retry tasks).
* **Deleting an `OwnedProxy` evicts its object shortly after.** The object
  is evicted in a background thread, instead of before `del` returns,
  because evicting inside the garbage collector could deadlock.
  `Store.close()` and program exit wait for pending evictions. Code that
  checks [`store.exists(key)`][proxystore.store.base.Store.exists] right
  after deleting an owned proxy may see the object for a moment longer.
* **`clone()` copies through the store.**
  [`clone()`][proxystore.store.ref.clone] copies the stored object through
  the store and returns a clone which is not resolved, even if the original
  is. In v1, the clone was given a deep copy of the original's local target,
  which could differ from the clone's stored copy. `clone()` raises
  [`ProxyResolveMissingKeyError`][proxystore.store.exceptions.ProxyResolveMissingKeyError]
  if the object is missing and
  [`MutableBorrowError`][proxystore.store.ref.MutableBorrowError] if the
  proxy is mutably borrowed.
* [`StoreExecutor.map()`][proxystore.store.executor.StoreExecutor.map]
  applies `should_proxy` to each element, like
  [`submit()`][proxystore.store.executor.StoreExecutor.submit], rather than
  to each iterable.
* References pickled by [`scopes.submit()`][proxystore.store.scopes.submit]
  are now released correctly. In v1, the owner's borrow count was never
  decremented for refs sent to a worker process.
* [`LeaseLifetime`][proxystore.store.lifetimes.LeaseLifetime] no longer
  blocks interpreter exit until the lease expires.

## Endpoints

!!! warning "Stop endpoints before upgrading"

    v2 cannot see endpoints started by v1, so it cannot stop them. Stop
    running endpoints with `proxystore-endpoint stop <name>` **before**
    upgrading.

ProxyStore Endpoints were rebuilt, so endpoints configured with v1 must be
stopped before upgrading and then configured again. The commands are below,
and the [Endpoints Guide](guides/endpoints.md#upgrading-from-proxystore-v1)
explains each step.

```bash
# 1. With ProxyStore v1 still installed
proxystore-endpoint stop my-endpoint

# 2. Upgrade ProxyStore, then configure the endpoint again
proxystore-endpoint remove my-endpoint
proxystore-endpoint configure my-endpoint
proxystore-endpoint peers add my-endpoint <peer-name> <peer-id>
proxystore-endpoint start my-endpoint
```

If you already upgraded, v2 cannot see the v1 endpoint's `daemon.pid` file,
so stop the old endpoint with `kill $(cat <endpoint-dir>/daemon.pid)`.

### What Changed

* **Clients talk to endpoints over an authenticated TCP protocol** instead of
  an unauthenticated HTTP API. In v1, anyone who could reach an endpoint's
  port could get, set, or evict objects. Clients now authenticate with a
  token stored in the endpoint directory, which only its owner can read.
  Connections between clients and endpoints use TLS by default (turn it off
  with `configure --no-tls`). Clients and endpoints from different major
  versions cannot talk to each other and raise a clear error in both
  directions.
* **Peering uses [iroh](https://iroh.computer){target=_blank} instead of
  WebRTC and the ProxyStore relay server.** The `proxystore-relay` CLI was
  removed. v1 and v2 endpoints cannot peer with each other, so upgrade
  endpoints that peer with each other together. By default, endpoints use
  n0's public relays and discovery service, which are third-party services.
  Use `configure --relays` and `--discovery` to disable them or to use
  self-hosted relays.
* **The ProxyStore relay server is no longer used.** v2 endpoints do not use
  the relay server hosted by the ProxyStore team. That relay server will
  stay online for at least six months after the release of v2.0.0 so v1
  endpoints keep working while you move between versions.
* **Endpoints are identified by an endpoint ID** (an ed25519 public key)
  instead of a UUID. Get the ID with `proxystore-endpoint id <name>`.
* **Peers must be allowed explicitly.** Each endpoint has an allowlist of
  peer IDs in `peers.toml`, managed with
  `proxystore-endpoint peers add|remove|list`.

### `EndpointConnector`

These changes affect the
[`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector].

* `endpoints` is a list of endpoint IDs instead of UUIDs. Update any UUIDs in
  your code or configs.
* `endpoints` is now optional. If omitted, the connector uses a running
  endpoint in the ProxyStore home directory.
* A new `reconnect_timeout` option controls how long the connector retries
  connecting to a restarting endpoint.
* The connector is thread-safe and fork-safe.

### CLI Changes

| v1 | v2 |
| --- | --- |
| `proxystore-endpoint configure --relay-address/--relay-auth/--relay-server/--peer-channels` | `--peering/--no-peering`, `--relays`, `--discovery`, `--tls/--no-tls` |
| `proxystore-endpoint check-nat` | Removed. |
| `proxystore-endpoint test --remote UUID <cmd>` | `proxystore-endpoint client --target ID <cmd>` |
| | New: `id`, `peers add/remove/list`, `client ping` |
| `proxystore-relay` | Removed. |

See the [CLI Reference](api/cli.md#proxystore-endpoint) for all commands.

### Python API

The [`proxystore.endpoint`][proxystore.endpoint] Python API is now
internal. Use endpoints through the `proxystore-endpoint` CLI and the
[`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector].

* `proxystore.p2p` was removed (peering now lives in internal endpoint
  modules).
* Endpoint exceptions now share an
  [`EndpointError`][proxystore.connectors.endpoint.EndpointError] base
  class.
* `EndpointVersionWarning` was replaced by
  [`VersionMismatchWarning`][proxystore.warnings.VersionMismatchWarning].

## Removed Modules and APIs

| Removed | Replacement |
| --- | --- |
| `proxystore.ex` | [`proxystore.connectors.daos`][proxystore.connectors.daos], [`proxystore.connectors.zmq`][proxystore.connectors.zmq] |
| `proxystore.connectors.dim` | [`proxystore.connectors.zmq`][proxystore.connectors.zmq] |
| `proxystore.factory` | Any callable, e.g., [`functools.partial`][functools.partial] |
| `proxystore.p2p` | None (internal to endpoints) |
| `proxystore-relay` CLI | None |
| `proxystore.store.future.Future` | [`proxystore.store.future.ProxyFuture`][proxystore.store.future.ProxyFuture] |
| Name-based store registration | See [Removed Registration API](#removed-registration-api) |

The following modules still exist but are now internal and may change in
any release (see [Public API](versioning.md#public-api)):
[`proxystore.utils`][proxystore.utils],
[`proxystore.globus`][proxystore.globus] (except the
`proxystore-globus-auth` CLI),
[`proxystore.store.cache`][proxystore.store.cache],
[`proxystore.endpoint`][proxystore.endpoint], and the server helpers in
[`proxystore.connectors.zmq`][proxystore.connectors.zmq].

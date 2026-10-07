"""Store implementation."""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Mapping
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import cast
from typing import Generic
from typing import Literal
from typing import overload
from typing import Self
from typing import TypeVar

import proxystore
import proxystore.serialize
from proxystore.connectors.protocols import DeferrableConnector
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import BytesLike
from proxystore.serialize import is_bytes_like
from proxystore.serialize import SerializationError
from proxystore.store.cache import LRUCache
from proxystore.store.config import ConnectorConfig
from proxystore.store.config import StoreConfig
from proxystore.store.exceptions import NonProxiableTypeError
from proxystore.store.factory import PollingStoreFactory
from proxystore.store.factory import StoreFactory
from proxystore.store.future import PollingPolicy
from proxystore.store.future import ProxyFuture
from proxystore.store.lifetimes import Lifetime
from proxystore.store.metrics import StoreMetrics
from proxystore.store.ref import into_owned
from proxystore.store.ref import OwnedProxy
from proxystore.store.registry import registry
from proxystore.store.types import ConnectorKeyT
from proxystore.store.types import ConnectorT
from proxystore.store.types import DeserializerT
from proxystore.store.types import SerializerT
from proxystore.utils.imports import get_object_path
from proxystore.utils.timer import Timer

logger = logging.getLogger(__name__)

T = TypeVar('T')

NonProxiableT = TypeVar('NonProxiableT', bool, None)
# These should be kept in sync with NonProxiableT
_NON_PROXIABLE_TYPES = (bool, type(None))

_MISSING_OBJECT = object()


class Store(Generic[ConnectorT]):
    r"""Key-value store interface for proxies.

    Tip:
        A [`Store`][proxystore.store.base.Store] instance can be used as a
        context manager which will automatically call
        [`close()`][proxystore.store.base.Store.close] on exit.

        ```python
        with Store(...) as store:
            key = store.put('value')
            store.get(key)
        ```

    Warning:
        The default value of `populate_target=True` can cause unexpected
        behavior when providing custom serializer/deserializers because
        neither the serializer nor deserializer will be applied to the target
        object being cached in the resulting [`Proxy`][proxystore.proxy.Proxy].

        ```python linenums="1"
        import pickle
        from proxystore.store import Store
        from proxystore.connectors.local import LocalConnector

        with Store(LocalConnector()) as store:
            data = [1, 2, 3]
            data_bytes = pickle.dumps(data)

            data_proxy = store.proxy(
                data_bytes,
                serializer=lambda s: s,
                deserializer=pickle.loads,
                populate_target=True,
            )

            print(data_proxy)
            # b'\x80\x04\x95\x0b\x00\x00\x00\x00\x00\x00\x00]\x94(K\x01K\x02K\x03e.'
        ```

        In this example, the serialized `data_bytes` was populated as the
        target object in the resulting proxy so the proxy looks like a proxy
        of bytes rather than the intended list of integers. To fix this, set
        `populate_target=False` so the custom deserializer is correctly
        applied to `data_bytes` when the proxy is resolved.

    Note:
        This class is generally thread-safe, with cache access and connector
        operations guarded by a lock that is local to each store instance.

    Note:
        Each store has a unique [`id`][proxystore.store.base.Store.id] which
        is used to register the store in a registry that is global to the
        Python process. The store is registered when initialized and
        unregistered when [`close()`][proxystore.store.base.Store.close]
        is called. Proxies created by a store contain the configuration of
        the store, including the ID, so the store can be found in the
        registry, or initialized if needed, when a proxy is resolved.

    Warning:
        This class cannot be pickled. If you need to recreate a
        [`Store`][proxystore.store.base.Store] within another process, share
        a [`StoreConfig`][proxystore.store.config.StoreConfig], a serializable
        and pickle-compatible type, that can be created using
        [`Store.config()`][proxystore.store.base.Store.config].

        To reconstruct the instance from the config, use
        [`get_or_create_store()`][proxystore.store.get_or_create_store],
        which reuses the store if it already exists in the process, or
        [`Store.from_config()`][proxystore.store.base.Store.from_config],
        which always creates a new store instance.

    Args:
        connector: Connector instance to use for object storage.
        name: Optional name of the store used in logs and error messages.
            The name does not need to be unique.
        serializer: Optional callable which serializes the object. If `None`,
            the default serializer
            ([`serialize()`][proxystore.serialize.serialize]) will be used.
        deserializer: Optional callable used by the factory to deserialize the
            byte string. If `None`, the default deserializer
            ([`deserialize()`][proxystore.serialize.deserialize]) will be
            used.
        cache_size: Size of LRU cache (in # of objects). If 0,
            the cache is disabled. The cache is local to the Python process.
        metrics: Enable recording operation metrics.
        populate_target: Set the default value of `populate_target` for
            proxy methods of the store.
        owner: This store owns the objects stored by the connector, so
            the connector is cleared (e.g., the directory of a
            [`FileConnector`][proxystore.connectors.file.FileConnector] is
            deleted), according to the connector's `clear` setting, when
            this store is closed. Stores created implicitly, such as when a
            proxy is resolved in another process, are never owners.

    Raises:
        ValueError: If `cache_size` is less than zero.
    """  # noqa: E501

    def __init__(
        self,
        connector: ConnectorT,
        *,
        name: str | None = None,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        cache_size: int = 16,
        metrics: bool = False,
        populate_target: bool = True,
        owner: bool = True,
        _id: str | None = None,
    ) -> None:
        # _id is private and only used by from_config() to recreate a store
        # with the same ID. Users should not set it because stores with the
        # same ID are considered the same store.
        if cache_size < 0:
            raise ValueError(
                f'Cache size cannot be negative. Got {cache_size}.',
            )

        self.connector = connector
        self.cache: LRUCache[ConnectorKeyT, Any] = LRUCache(cache_size)
        self._id = _id if _id is not None else uuid.uuid4().hex
        self._name = name
        self._metrics = StoreMetrics() if metrics else None
        self._cache_size = cache_size
        self._serializer = serializer
        self._deserializer = deserializer
        self._populate_target = populate_target
        self._owner = owner
        self._lock = threading.RLock()

        registry.register(self)

        logger.info('Initialized %s', self)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        config = self.config().model_dump()

        del config['id']
        del config['name']
        del config['connector']

        config['serializer'] = (
            'default' if config['serializer'] is None else 'custom'
        )
        config['deserializer'] = (
            'default' if config['deserializer'] is None else 'custom'
        )
        config['metrics'] = self.metrics is not None

        params = ', '.join(f'{k}={v}' for k, v in config.items())
        name = '' if self.name is None else f'name={self.name}, '
        return (
            f'Store(id={self.id}, {name}connector={self.connector}, {params})'
        )

    @property
    def id(self) -> str:
        """Unique ID of this [`Store`][proxystore.store.base.Store].

        The ID is shared by stores initialized from the
        [`config()`][proxystore.store.base.Store.config] of this store
        (e.g., when a proxy created by this store is resolved in another
        process).
        """
        return self._id

    @property
    def owner(self) -> bool:
        """This store owns the objects stored by its connector.

        Only the owner clears the connector when closed. See
        [`close()`][proxystore.store.base.Store.close].
        """
        return self._owner

    @property
    def name(self) -> str | None:
        """Optional name of this [`Store`][proxystore.store.base.Store]."""
        return self._name

    @property
    def _label(self) -> str:
        # Name used in logs.
        return self.id if self.name is None else self.name

    @property
    def metrics(self) -> StoreMetrics | None:
        """Optional metrics for this instance."""
        return self._metrics

    @property
    def serializer(self) -> SerializerT:
        """Serializer for this instance."""
        return (
            self._serializer
            if self._serializer is not None
            else proxystore.serialize.serialize
        )

    @property
    def deserializer(self) -> DeserializerT:
        """Deserializer for this instance."""
        return (
            self._deserializer
            if self._deserializer is not None
            else proxystore.serialize.deserialize
        )

    def close(self, *, clear: bool | None = None) -> None:
        """Close the connector associated with the store.

        This will (1) close the connector and (2) unregister the store.
        If this store is the [`owner`][proxystore.store.base.Store.owner]
        and the connector removed the stored objects, the ID of the store
        stays closed for the rest of the process, so resolving proxies of
        the store in this process raises a
        [`StoreClosedError`][proxystore.store.exceptions.StoreClosedError]
        instead of making a new store.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.

        Args:
            clear: Clear the objects stored by the connector (see
                [`Connector.close()`][proxystore.connectors.protocols.Connector.close]).
                If `None`, the connector's default is used if this store is
                the [`owner`][proxystore.store.base.Store.owner] and the
                connector is not cleared otherwise.
        """
        if clear is None and not self.owner:
            clear = False

        cleared: bool | None = None
        try:
            with self._lock:
                if clear is None:
                    cleared = self.connector.close()
                else:
                    cleared = self.connector.close(clear=clear)
        finally:
            registry.unregister(self, closed=self.owner and cleared is True)

    def config(self) -> StoreConfig:
        """Get the store configuration.

        Example:
            ```python
            >>> store = Store(...)
            >>> config = store.config()
            >>> store = Store.from_config(config)
            ```

        Returns:
            Store configuration.
        """
        return StoreConfig(
            id=self.id,
            name=self.name,
            connector=ConnectorConfig(
                kind=get_object_path(type(self.connector)),
                options=self.connector.config(),
            ),
            serializer=self._serializer,
            deserializer=self._deserializer,
            cache_size=self._cache_size,
            metrics=self.metrics is not None,
            populate_target=self._populate_target,
        )

    @classmethod
    def from_config(
        cls,
        config: StoreConfig,
        *,
        owner: bool | None = None,
    ) -> Store[Any]:
        """Create a new store instance from a configuration.

        The new store has the same [`id`][proxystore.store.base.Store.id]
        as the store which produced the configuration. If `config` does not
        contain an ID (e.g., a configuration loaded from a TOML file),
        a new ID is generated.

        By default, the new store is the
        [`owner`][proxystore.store.base.Store.owner] only if `config` does
        not contain an ID. A config with an ID came from an existing store,
        and that store is the owner. A config without an ID describes a new
        store, so the new store is the owner.

        Tip:
            Use [`get_or_create_store()`][proxystore.store.get_or_create_store]
            to reuse a store with the same ID that already exists in this
            process.

        Args:
            config: Configuration returned by `#!python .config()`.
            owner: The new store owns the objects stored by the connector.
                See the `owner` parameter of
                [`Store`][proxystore.store.base.Store]. If `None`, the new
                store is the owner only if `config` does not contain an ID.

        Returns:
            Store instance.
        """
        if owner is None:
            owner = config.id is None
        connector = cast(ConnectorT, config.connector.get_connector())
        return cls(
            connector,
            name=config.name,
            serializer=config.serializer,
            deserializer=config.deserializer,
            cache_size=config.cache_size,
            metrics=config.metrics,
            populate_target=config.populate_target,
            owner=owner,
            _id=config.id,
        )

    def future(
        self,
        *,
        evict: bool = False,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        polling: PollingPolicy | None = None,
    ) -> ProxyFuture[T]:
        """Create a future to an object.

        Example:
            ```python
            from proxystore.connectors.file import FileConnector
            from proxystore.store import Store
            from proxystore.store.future import ProxyFuture

            def remote_foo(future: ProxyFuture) -> None:
                # Computation that generates a result value needed by
                # the remote_bar function.
                future.set_result(...)

            def remote_bar(data: Any) -> None:
                # Function uses data, which is a proxy, as normal, blocking
                # until the remote_foo function has called set_result.
                ...

            with Store(FileConnector(...)) as store:
                future = store.future()

                # The invoke_remote function invokes a provided function
                # on a remote process. For example, this could be a serverless
                # function execution.
                foo_result_future = invoke_remote(remote_foo, future)
                bar_result_future = invoke_remote(remote_bar, future.proxy())

                foo_result_future.result()
                bar_result_future.result()
            ```

        Warning:
            This method only works if the `connector` is of type
            [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector].

        Args:
            evict: If a proxy returned by
                [`ProxyFuture.proxy()`][proxystore.store.future.ProxyFuture.proxy]
                should evict the object once resolved.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            polling: Policy for polling the store for the result of the
                future. If `None`, the default
                [`PollingPolicy`][proxystore.store.future.PollingPolicy] is
                used.

        Returns:
            Future which can be used to get the result object at a later time \
            or create a proxy which will resolve to the result of the future.

        Raises:
            NotImplementedError: If the `connector` is not of type
                [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector].
        """
        timer = Timer().start()

        if not isinstance(self.connector, DeferrableConnector):
            raise NotImplementedError(
                'The provided connector is type '
                f'{type(self.connector).__name__} which does not implement '
                f'the {DeferrableConnector.__name__} necessary to use the '
                f'{ProxyFuture.__name__} interface.',
            )

        with Timer() as connector_timer:
            key = self.connector.new_key()

        if self.metrics is not None:
            ctime = connector_timer.elapsed_ms
            self.metrics.add_time('store.future.connector', key, ctime)

        factory: PollingStoreFactory[ConnectorT, T] = PollingStoreFactory(
            key,
            store_config=self.config(),
            deserializer=deserializer,
            evict=evict,
            polling=polling,
        )
        future = ProxyFuture(factory, serializer=serializer)

        timer.stop()
        if self.metrics is not None:
            self.metrics.add_time('store.future', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): FUTURE %s in %.3f ms',
            self._label,
            key,
            timer.elapsed_ms,
        )
        return future

    def evict(self, key: ConnectorKeyT) -> None:
        """Evict the object associated with the key.

        Args:
            key: Key associated with object to evict.
        """
        timer = Timer().start()

        with self._lock:
            with Timer() as connector_timer:
                self.connector.evict(key)

            if self.metrics is not None:
                ctime = connector_timer.elapsed_ms
                self.metrics.add_time('store.evict.connector', key, ctime)

            self.cache.evict(key)

        timer.stop()
        if self.metrics is not None:
            self.metrics.add_time('store.evict', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): EVICT %s in %.3f ms',
            self._label,
            key,
            timer.elapsed_ms,
        )

    def exists(self, key: ConnectorKeyT) -> bool:
        """Check if an object associated with the key exists.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If an object associated with the key exists.
        """
        timer = Timer().start()

        with self._lock:
            res = self.cache.exists(key)
            if not res:
                with Timer() as connector_timer:
                    res = self.connector.exists(key)

                if self.metrics is not None:
                    ctime = connector_timer.elapsed_ms
                    self.metrics.add_time('store.exists.connector', key, ctime)

        timer.stop()
        if self.metrics is not None:
            self.metrics.add_time('store.exists', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): EXISTS %s in %.3f ms',
            self._label,
            key,
            timer.elapsed_ms,
        )
        return res

    def get(
        self,
        key: ConnectorKeyT,
        *,
        deserializer: DeserializerT | None = None,
        default: object | None = None,
    ) -> Any | None:
        """Get the object associated with the key.

        Tip:
            Like [`dict.get()`][dict.get], `None` is returned if the object
            does not exist, so a missing object cannot be distinguished from
            an object which is `None`. Use a sentinel `default` or
            [`exists()`][proxystore.store.base.Store.exists] if you need
            to distinguish the two.

            ```python
            missing = object()
            obj = store.get(key, default=missing)
            if obj is missing:
                ...
            ```

        Args:
            key: Key associated with the object to retrieve.
            deserializer: Optionally override the default deserializer for the
                store instance.
            default: An optional value to be returned if an object
                associated with the key does not exist.

        Returns:
            Object or `default` if the object does not exist.

        Raises:
            SerializationError: If an exception is caught when deserializing
                the object associated with the key.
        """
        timer = Timer().start()

        with self._lock:
            cached = self.cache.get(key, _MISSING_OBJECT)
            if cached is not _MISSING_OBJECT:
                timer.stop()
                if self.metrics is not None:
                    self.metrics.add_counter('store.get.cache_hits', key, 1)
                    self.metrics.add_time('store.get', key, timer.elapsed_ms)

                logger.debug(
                    'Store(%s): GET %s in %.3f ms (cached=True)',
                    self._label,
                    key,
                    timer.elapsed_ms,
                )
                return cached

            with Timer() as connector_timer:
                value = self.connector.get(key)

            if self.metrics is not None:
                ctime = connector_timer.elapsed_ms
                self.metrics.add_counter('store.get.cache_misses', key, 1)
                self.metrics.add_time('store.get.connector', key, ctime)

            if value is not None:
                with Timer() as deserializer_timer:
                    deserializer = (
                        deserializer
                        if deserializer is not None
                        else self.deserializer
                    )
                    try:
                        result = deserializer(value)
                    except Exception as e:
                        name = get_object_path(deserializer)
                        raise SerializationError(
                            'Failed to deserialize object '
                            f'(deserializer={name}, key={key}).',
                        ) from e

                if self.metrics is not None:
                    dtime = deserializer_timer.elapsed_ms
                    obj_size = len(value)
                    self.metrics.add_time('store.get.deserialize', key, dtime)
                    self.metrics.add_attribute(
                        'store.get.object_size',
                        key,
                        obj_size,
                    )

                self.cache.set(key, result)
            else:
                result = default

        timer.stop()
        if self.metrics is not None:
            self.metrics.add_time('store.get', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): GET %s in %.3f ms (cached=False)',
            self._label,
            key,
            timer.elapsed_ms,
        )
        return result

    def get_batch(
        self,
        keys: Sequence[ConnectorKeyT],
        *,
        deserializer: DeserializerT | None = None,
        default: object | None = None,
    ) -> list[Any | None]:
        """Get the objects associated with the keys.

        Cached objects are returned from the cache, and the remaining objects
        are retrieved with a single call to
        [`Connector.get_batch()`][proxystore.connectors.protocols.Connector.get_batch].

        Args:
            keys: Sequence of keys associated with the objects to retrieve.
            deserializer: Optionally override the default deserializer for the
                store instance.
            default: An optional value to be returned for each object
                that does not exist.

        Returns:
            List with the same order as `keys` containing each object or \
            `default` if the object does not exist.

        Raises:
            SerializationError: If an exception is caught when deserializing
                an object.
        """
        timer = Timer().start()
        deserializer = (
            deserializer if deserializer is not None else self.deserializer
        )
        results: list[Any] = [default] * len(keys)

        with self._lock:
            misses: list[int] = []
            for i, key in enumerate(keys):
                cached = self.cache.get(key, _MISSING_OBJECT)
                if cached is _MISSING_OBJECT:
                    misses.append(i)
                else:
                    results[i] = cached

            if len(misses) > 0:
                with Timer() as connector_timer:
                    values = self.connector.get_batch(
                        [keys[i] for i in misses],
                    )

                with Timer() as deserializer_timer:
                    for i, value in zip(misses, values, strict=True):
                        if value is None:
                            continue
                        try:
                            result = deserializer(value)
                        except Exception as e:
                            name = get_object_path(deserializer)
                            raise SerializationError(
                                'Failed to deserialize object '
                                f'(deserializer={name}, key={keys[i]}).',
                            ) from e
                        self.cache.set(keys[i], result)
                        results[i] = result

        timer.stop()
        if self.metrics is not None:
            hits = len(keys) - len(misses)
            self.metrics.add_counter('store.get_batch.cache_hits', keys, hits)
            self.metrics.add_counter(
                'store.get_batch.cache_misses',
                keys,
                len(misses),
            )
            if len(misses) > 0:
                ctime = connector_timer.elapsed_ms
                dtime = deserializer_timer.elapsed_ms
                self.metrics.add_time('store.get_batch.connector', keys, ctime)
                self.metrics.add_time(
                    'store.get_batch.deserialize',
                    keys,
                    dtime,
                )
            self.metrics.add_time('store.get_batch', keys, timer.elapsed_ms)

        logger.debug(
            'Store(%s): GET_BATCH (%s items, %s cached) in %.3f ms',
            self._label,
            len(keys),
            len(keys) - len(misses),
            timer.elapsed_ms,
        )
        return results

    def is_cached(self, key: ConnectorKeyT) -> bool:
        """Check if an object associated with the key is cached locally.

        Args:
            key: Key potentially associated with stored object.

        Returns:
            If the object is cached.
        """
        with self._lock:
            return self.cache.exists(key)

    # The first overload overlaps with the second because a NonProxiableT
    # is also a T, but the first overload is matched first.
    @overload
    def proxy(  # type: ignore[overload-overlap]
        self,
        obj: NonProxiableT,
        *,
        evict: bool = ...,
        lifetime: Lifetime | None = ...,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: Literal[True] = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> NonProxiableT: ...

    @overload
    def proxy(
        self,
        obj: T,
        *,
        evict: bool = ...,
        lifetime: Lifetime | None = ...,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: bool = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> Proxy[T]: ...

    def proxy(
        self,
        obj: T | NonProxiableT,
        *,
        evict: bool = False,
        lifetime: Lifetime | None = None,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        populate_target: bool | None = None,
        skip_nonproxiable: bool = True,
        connector_options: Mapping[str, Any] | None = None,
    ) -> Proxy[T] | NonProxiableT:
        """Create a proxy that will resolve to an object in the store.

        Args:
            obj: The object to place in store and return a proxy for.
            evict: If the proxy should evict the object once resolved.
                Mutually exclusive with the `lifetime` parameter.
            lifetime: Attach the proxy to this lifetime. The object associated
                with the proxy will be evicted when the lifetime ends.
                Mutually exclusive with the `evict` parameter.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            populate_target: Pass `cache_defaults=True` and `target=obj` to
                the [`Proxy`][proxystore.proxy.Proxy] constructor. I.e.,
                return a proxy that (1) is already resolved, (2) can be used
                in [`isinstance`][isinstance] checks without resolving, and (3)
                is hashable without resolving if `obj` is a hashable type.
                Note that the returned proxy will hold a reference to `obj`
                which will prevent garbage collecting `obj`. If `None`,
                defaults to the store-wide setting.
            skip_nonproxiable: Return non-proxiable types (e.g., built-in
                constants like `bool` or `None`) directly. If `False`, a
                [`NonProxiableTypeError`][proxystore.store.exceptions.NonProxiableTypeError]
                is raised instead.
            connector_options: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.protocols.Connector.put].

        Returns:
            A proxy of the object unless `obj` is a non-proxiable type \
            and `#!python skip_nonproxiable is True` in which case `obj` is \
            returned directly.

        Raises:
            NonProxiableTypeError: If `obj` is a non-proxiable type and
                `#!python skip_nonproxiable=False`.
            ValueError: If `evict` is `True` and `lifetime` is not `None`
                because these parameters are mutually exclusive.
        """
        if evict and lifetime is not None:
            raise ValueError(
                'The evict and lifetime parameters are mutually exclusive. '
                'Only set one of evict or lifetime.',
            )

        if isinstance(obj, _NON_PROXIABLE_TYPES):
            if skip_nonproxiable:
                # MyPy raises the following error which is not correct:
                #     Incompatible return value type (got "Optional[bool]",
                #     expected "Optional[Proxy[T]]")  [return-value]
                return obj  # type: ignore[return-value]
            raise NonProxiableTypeError(
                f'Object of {type(obj)} is not proxiable.',
            )

        with Timer() as timer:
            key = self.put(
                obj,
                serializer=serializer,
                connector_options=connector_options,
            )
            factory: StoreFactory[ConnectorT, T] = StoreFactory(
                key,
                store_config=self.config(),
                deserializer=deserializer,
                evict=evict,
            )
            populate_target = (
                self._populate_target
                if populate_target is None
                else populate_target
            )
            if populate_target:
                # If obj were None, we would have escaped early when
                # checking _NON_PROXIABLE_TYPES.
                assert obj is not None
                proxy = Proxy(factory, cache_defaults=True, target=obj)
            else:
                proxy = Proxy(factory)

            if lifetime is not None:
                lifetime.add_proxy(proxy)

        if self.metrics is not None:
            self.metrics.add_time('store.proxy', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): PROXY %s in %.3f ms',
            self._label,
            key,
            timer.elapsed_ms,
        )
        return proxy

    # The first overload overlaps with the second because a NonProxiableT
    # is also a T, but the first overload is matched first.
    @overload
    def proxy_batch(  # type: ignore[overload-overlap]
        self,
        objs: Sequence[NonProxiableT],
        *,
        evict: bool = ...,
        lifetime: Lifetime | None = ...,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: Literal[True] = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> list[NonProxiableT]: ...

    @overload
    def proxy_batch(
        self,
        objs: Sequence[T],
        *,
        evict: bool = ...,
        lifetime: Lifetime | None = ...,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: bool = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> list[Proxy[T]]: ...

    # MyPy raises the following:
    #    Overloaded function implementation cannot produce return type of
    #    signature 1
    def proxy_batch(  # type: ignore[misc]
        self,
        objs: Sequence[T | NonProxiableT],
        *,
        evict: bool = False,
        lifetime: Lifetime | None = None,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        populate_target: bool | None = None,
        skip_nonproxiable: bool = True,
        connector_options: Mapping[str, Any] | None = None,
    ) -> list[Proxy[T] | NonProxiableT]:
        """Create proxies that will resolve to an object in the store.

        Args:
            objs: The objects to place in store and return a proxies for.
            evict: If a proxy should evict its object once resolved.
                Mutually exclusive with the `lifetime` parameter.
            lifetime: Attach the proxies to this lifetime. The objects
                associated with each proxy will be evicted when the lifetime
                ends. Mutually exclusive with the `evict` parameter.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            populate_target: Pass `cache_defaults=True` and `target=obj` to
                the [`Proxy`][proxystore.proxy.Proxy] constructor. I.e.,
                return a proxy that (1) is already resolved, (2) can be used
                in [`isinstance`][isinstance] checks without resolving, and (3)
                is hashable without resolving if `obj` is a hashable type.
                If `None`, defaults to the store-wide setting.
            skip_nonproxiable: Return non-proxiable types (e.g., built-in
                constants like `bool` or `None`) directly. If `False`, a
                [`NonProxiableTypeError`][proxystore.store.exceptions.NonProxiableTypeError]
                is raised instead.
            connector_options: Additional keyword arguments to pass to
                [`Connector.put_batch()`][proxystore.connectors.protocols.Connector.put_batch].

        Returns:
            A list of proxies of each object or the object itself if said \
            object is not proxiable and `#!python skip_nonproxiable is True`.

        Raises:
            NonProxiableTypeError: If `obj` is a non-proxiable type and
                `#!python skip_nonproxiable=False`.
            ValueError: If `evict` is `True` and `lifetime` is not `None`
                because these parameters are mutually exclusive.
        """
        if evict and lifetime is not None:
            raise ValueError(
                'The evict and lifetime parameters are mutually exclusive. '
                'Only set one of evict or lifetime.',
            )

        with Timer() as timer:
            # Find if there are non-proxiable types and if that's okay
            non_proxiable: list[tuple[int, Any]] = []
            for i, obj in enumerate(objs):
                if isinstance(obj, _NON_PROXIABLE_TYPES):
                    non_proxiable.append((i, obj))

            if len(non_proxiable) > 0 and not skip_nonproxiable:
                raise NonProxiableTypeError(
                    f'Input sequence contains {len(non_proxiable)} '
                    'objects that are not proxiable.',
                )

            # Pop non-proxiable types so we can batch proxy the proxiable ones
            non_proxiable_indicies = [i for i, _ in non_proxiable]
            proxiable_objs = [
                obj
                for i, obj in enumerate(objs)
                if i not in non_proxiable_indicies
            ]

            keys = self.put_batch(
                proxiable_objs,
                serializer=serializer,
                connector_options=connector_options,
            )
            factories: list[StoreFactory[ConnectorT, T]] = [
                StoreFactory(
                    key,
                    store_config=self.config(),
                    evict=evict,
                    deserializer=deserializer,
                )
                for key in keys
            ]

            populate_target = (
                self._populate_target
                if populate_target is None
                else populate_target
            )

            proxies: list[Proxy[T]] = []
            for factory, obj in zip(factories, proxiable_objs, strict=True):
                if populate_target:
                    proxy = Proxy(factory, cache_defaults=True, target=obj)
                else:
                    proxy = Proxy(factory)
                proxies.append(proxy)

            if lifetime is not None:
                lifetime.add_proxy(*proxies)

            # Put non-proxiable objects back in their original positions.
            # The indices of non_proxiable must still be sorted
            for original_index, original_object in non_proxiable:
                proxies.insert(original_index, original_object)

        if self.metrics is not None:
            self.metrics.add_time('store.proxy_batch', keys, timer.elapsed_ms)

        logger.debug(
            'Store(%s): PROXY_BATCH (%s items) in %.3f ms',
            self._label,
            len(proxies),
            timer.elapsed_ms,
        )
        return cast(list[Proxy[T] | NonProxiableT], proxies)

    def proxy_from_key(
        self,
        key: ConnectorKeyT,
        *,
        evict: bool = False,
        lifetime: Lifetime | None = None,
        deserializer: DeserializerT | None = None,
    ) -> Proxy[T]:
        """Create a proxy that will resolve to an object already in the store.

        Args:
            key: The key associated with an object already in the store.
            evict: If the proxy should evict the object once resolved.
                Mutually exclusive with the `lifetime` parameter.
            lifetime: Attach the proxy to this lifetime. The object associated
                with the proxy will be evicted when the lifetime ends.
                Mutually exclusive with the `evict` parameter.
            deserializer: Optionally override the default deserializer for the
                store instance.

        Returns:
            A proxy of the object.

        Raises:
            ValueError: If `evict` is `True` and `lifetime` is not `None`
                because these parameters are mutually exclusive.
        """
        if evict and lifetime is not None:
            raise ValueError(
                'The evict and lifetime parameters are mutually exclusive. '
                'Only set one of evict or lifetime.',
            )

        factory: StoreFactory[ConnectorT, T] = StoreFactory(
            key,
            store_config=self.config(),
            deserializer=deserializer,
            evict=evict,
        )
        proxy = Proxy(factory)

        logger.debug('Store(%s): PROXY_FROM_KEY %s', self._label, key)

        if lifetime is not None:
            lifetime.add_proxy(proxy)

        return proxy

    # The first overload overlaps with the second because a NonProxiableT
    # is also a T, but the first overload is matched first.
    @overload
    def locked_proxy(  # type: ignore[overload-overlap]
        self,
        obj: NonProxiableT,
        *,
        evict: bool = ...,
        lifetime: Lifetime | None = ...,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: Literal[True] = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> NonProxiableT: ...

    @overload
    def locked_proxy(
        self,
        obj: T,
        *,
        evict: bool = ...,
        lifetime: Lifetime | None = ...,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: bool = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> ProxyLocker[T]: ...

    def locked_proxy(
        self,
        obj: T | NonProxiableT,
        *,
        evict: bool = False,
        lifetime: Lifetime | None = None,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        populate_target: bool | None = None,
        skip_nonproxiable: bool = True,
        connector_options: Mapping[str, Any] | None = None,
    ) -> ProxyLocker[T] | NonProxiableT:
        """Proxy an object and return [`ProxyLocker`][proxystore.proxy.ProxyLocker].

        Args:
            obj: The object to place in store and return a proxy for.
            evict: If the proxy should evict the object once resolved.
                Mutually exclusive with the `lifetime` parameter.
            lifetime: Attach the proxy to this lifetime. The object associated
                with the proxy will be evicted when the lifetime ends.
                Mutually exclusive with the `evict` parameter.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            populate_target: Pass `cache_defaults=True` and `target=obj` to
                the [`Proxy`][proxystore.proxy.Proxy] constructor. I.e.,
                return a proxy that (1) is already resolved, (2) can be used
                in [`isinstance`][isinstance] checks without resolving, and (3)
                is hashable without resolving if `obj` is a hashable type.
                If `None`, defaults to the store-wide setting.
            skip_nonproxiable: Return non-proxiable types (e.g., built-in
                constants like `bool` or `None`) directly. If `False`, a
                [`NonProxiableTypeError`][proxystore.store.exceptions.NonProxiableTypeError]
                is raised instead.
            connector_options: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.protocols.Connector.put].

        Returns:
            A proxy wrapped in a \
            [`ProxyLocker`][proxystore.proxy.ProxyLocker] unless `obj` is a \
            non-proxiable type and `#!python skip_nonproxiable is True` in which \
            case `obj` is returned directly.

        Raises:
            NonProxiableTypeError: If `obj` is a non-proxiable type and
                `#!python skip_nonproxiable=False`.
            ValueError: If `evict` is `True` and `lifetime` is not `None`
                because these parameters are mutually exclusive.
        """  # noqa: E501
        possible_proxy = self.proxy(
            obj,
            evict=evict,
            lifetime=lifetime,
            serializer=serializer,
            deserializer=deserializer,
            populate_target=populate_target,
            skip_nonproxiable=skip_nonproxiable,
            connector_options=connector_options,
        )

        if isinstance(possible_proxy, Proxy):
            return ProxyLocker(possible_proxy)
        return possible_proxy

    # The first overload overlaps with the second because a NonProxiableT
    # is also a T, but the first overload is matched first.
    @overload
    def owned_proxy(  # type: ignore[overload-overlap]
        self,
        obj: NonProxiableT,
        *,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: Literal[True] = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> NonProxiableT: ...

    @overload
    def owned_proxy(
        self,
        obj: T,
        *,
        serializer: SerializerT | None = ...,
        deserializer: DeserializerT | None = ...,
        populate_target: bool | None = ...,
        skip_nonproxiable: bool = ...,
        connector_options: Mapping[str, Any] | None = ...,
    ) -> OwnedProxy[T]: ...

    def owned_proxy(
        self,
        obj: T | NonProxiableT,
        *,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        populate_target: bool | None = None,
        skip_nonproxiable: bool = True,
        connector_options: Mapping[str, Any] | None = None,
    ) -> OwnedProxy[T] | NonProxiableT:
        """Create a proxy that will enforce ownership rules over the object.

        An [`OwnedProxy`][proxystore.store.ref.OwnedProxy] will auto-evict
        the object once it goes out of scope. This proxy type can also
        be borrowed.

        Args:
            obj: The object to place in store and return a proxy for.
            serializer: Optionally override the default serializer for the
                store instance.
            deserializer: Optionally override the default deserializer for the
                store instance.
            populate_target: Pass `cache_defaults=True` and `target=obj` to
                the [`Proxy`][proxystore.proxy.Proxy] constructor. I.e.,
                return a proxy that (1) is already resolved, (2) can be used
                in [`isinstance`][isinstance] checks without resolving, and (3)
                is hashable without resolving if `obj` is a hashable type.
                If `None`, defaults to the store-wide setting.
            skip_nonproxiable: Return non-proxiable types (e.g., built-in
                constants like `bool` or `None`) directly. If `False`, a
                [`NonProxiableTypeError`][proxystore.store.exceptions.NonProxiableTypeError]
                is raised instead.
            connector_options: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.protocols.Connector.put].

        Returns:
            A proxy of the object unless `obj` is a non-proxiable type \
            and `#!python skip_nonproxiable is True` in which case `obj` is \
            returned directly.

        Raises:
            NonProxiableTypeError: If `obj` is a non-proxiable type and
                `#!python skip_nonproxiable=False`.
        """
        possible_proxy = self.proxy(
            obj,
            evict=False,
            serializer=serializer,
            deserializer=deserializer,
            populate_target=populate_target,
            skip_nonproxiable=skip_nonproxiable,
            connector_options=connector_options,
        )

        if isinstance(possible_proxy, Proxy):
            populate_target = (
                self._populate_target
                if populate_target is None
                else populate_target
            )
            return into_owned(possible_proxy, populate_target=populate_target)
        return possible_proxy

    def put(
        self,
        obj: Any,
        *,
        lifetime: Lifetime | None = None,
        serializer: SerializerT | None = None,
        connector_options: Mapping[str, Any] | None = None,
    ) -> ConnectorKeyT:
        """Put an object in the store.

        Args:
            obj: Object to put in the store.
            serializer: Optionally override the default serializer for the
                store instance.
            lifetime: Attach the key to this lifetime. The object associated
                with the key will be evicted when the lifetime ends.
            connector_options: Additional keyword arguments to pass to
                [`Connector.put()`][proxystore.connectors.protocols.Connector.put].

        Returns:
            A key which can be used to retrieve the object.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        timer = Timer().start()

        with Timer() as serialize_timer:
            if serializer is not None:
                obj = serializer(obj)
            else:
                obj = self.serializer(obj)

        if not is_bytes_like(obj):
            raise TypeError('Serializer must return a bytes-like object.')

        with self._lock, Timer() as connector_timer:
            key = self.connector.put(obj, **(connector_options or {}))

        if lifetime is not None:
            lifetime.add_key(key, store=self)

        timer.stop()
        if self.metrics is not None:
            ctime = connector_timer.elapsed_ms
            stime = serialize_timer.elapsed_ms
            self.metrics.add_attribute('store.put.object_size', key, len(obj))
            self.metrics.add_time('store.put.serialize', key, stime)
            self.metrics.add_time('store.put.connector', key, ctime)
            self.metrics.add_time('store.put', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): PUT %s in %.3f ms',
            self._label,
            key,
            timer.elapsed_ms,
        )
        return key

    def put_batch(
        self,
        objs: Sequence[Any],
        *,
        lifetime: Lifetime | None = None,
        serializer: SerializerT | None = None,
        connector_options: Mapping[str, Any] | None = None,
    ) -> list[ConnectorKeyT]:
        """Put multiple objects in the store.

        Args:
            objs: Sequence of objects to put in the store.
            serializer: Optionally override the default serializer for the
                store instance.
            lifetime: Attach the keys to this lifetime. The objects associated
                with each key will be evicted when the lifetime ends.
            connector_options: Additional keyword arguments to pass to
                [`Connector.put_batch()`][proxystore.connectors.protocols.Connector.put_batch].

        Returns:
            A list of keys which can be used to retrieve the objects.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        timer = Timer().start()

        def _serialize(obj: Any) -> BytesLike:
            if serializer is not None:
                obj = serializer(obj)
            else:
                obj = self.serializer(obj)

            if not is_bytes_like(obj):
                raise TypeError('Serializer must return a bytes-like object.')

            return obj

        with Timer() as serialize_timer:
            _objs = list(map(_serialize, objs))

        with self._lock, Timer() as connector_timer:
            keys = self.connector.put_batch(
                _objs,
                **(connector_options or {}),
            )

        if lifetime is not None:
            lifetime.add_key(*keys, store=self)

        timer.stop()
        if self.metrics is not None:
            ctime = connector_timer.elapsed_ms
            stime = serialize_timer.elapsed_ms
            sizes = sum(len(obj) for obj in _objs)
            self.metrics.add_attribute(
                'store.put_batch.object_sizes',
                keys,
                sizes,
            )
            self.metrics.add_time('store.put_batch.serialize', keys, stime)
            self.metrics.add_time('store.put_batch.connector', keys, ctime)
            self.metrics.add_time('store.put_batch', keys, timer.elapsed_ms)

        logger.debug(
            'Store(%s): PUT_BATCH (%s items) in %.3f ms',
            self._label,
            len(keys),
            timer.elapsed_ms,
        )
        return keys

    def _set(
        self,
        key: ConnectorKeyT,
        obj: Any,
        *,
        serializer: SerializerT | None = None,
        connector_options: Mapping[str, Any] | None = None,
    ) -> None:
        """Set a key in the store to an object.

        Warning:
            This method only works if the `connector` is of type
            [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector].

        Warning:
            Associated [`Store`][proxystore.store.base.Store] instances in
            other processes may still have the old version of the object
            associated with `key` cached. This method is unable to invalidate
            those caches.

        Args:
            key: Key to set the object on.
            obj: Object to put in the store.
            serializer: Optionally override the default serializer for the
                store instance.
            connector_options: Additional keyword arguments to pass to
                [`DeferrableConnector.set()`][proxystore.connectors.protocols.DeferrableConnector.set].

        Raises:
            NotImplementedError: If the `connector` is not of type
                [`DeferrableConnector`][proxystore.connectors.protocols.DeferrableConnector].
            TypeError: If the output of `serializer` is not bytes.
        """
        if not isinstance(self.connector, DeferrableConnector):
            raise NotImplementedError(
                'The provided connector is type '
                f'{type(self.connector).__name__} which does not implement '
                f'the {DeferrableConnector.__name__} necessary to use the '
                'set method.',
            )

        timer = Timer().start()

        with Timer() as serialize_timer:
            if serializer is not None:
                obj = serializer(obj)
            else:
                obj = self.serializer(obj)

        if not is_bytes_like(obj):
            raise TypeError('Serializer must return a bytes-like object.')

        with self._lock:
            with Timer() as connector_timer:
                self.connector.set(key, obj, **(connector_options or {}))

            self.cache.evict(key)

        timer.stop()
        if self.metrics is not None:
            ctime = connector_timer.elapsed_ms
            stime = serialize_timer.elapsed_ms
            self.metrics.add_attribute('store.set.object_size', key, len(obj))
            self.metrics.add_time('store.set.serialize', key, stime)
            self.metrics.add_time('store.set.connector', key, ctime)
            self.metrics.add_time('store.set', key, timer.elapsed_ms)

        logger.debug(
            'Store(%s): SET %s in %.3f ms',
            self._label,
            key,
            timer.elapsed_ms,
        )

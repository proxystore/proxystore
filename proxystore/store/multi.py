"""MultiStore Implementation."""
from __future__ import annotations

import dataclasses
import logging
import sys
import warnings
from typing import Any
from typing import Callable
from typing import Iterable
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import TypedDict
else:  # pragma: <3.8 cover
    from typing_extensions import TypedDict

from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import serialize as default_serializer
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.warnings import ExperimentalWarning

warnings.warn(
    'MultiStore is an experimental feature and may change in the future.',
    category=ExperimentalWarning,
    stacklevel=2,
)

logger = logging.getLogger(__name__)
SerializerT = Callable[[Any], bytes]
DeserializerT = Callable[[bytes], Any]
T = TypeVar('T')


def _identity(x: T) -> T:
    return x


class PolicyDict(TypedDict):
    """JSON compatible representation of a [`Policy`][proxystore.store.multi.Policy]."""  # noqa: E501

    priority: int
    min_size: int
    max_size: int
    subset_tags: list[str]
    superset_tags: list[str]


@dataclasses.dataclass
class Policy:
    """Policy that allows validating a set of constraints."""

    priority: int = 0
    min_size: int = 0
    max_size: int = sys.maxsize
    subset_tags: list[str] = dataclasses.field(default_factory=list)
    superset_tags: list[str] = dataclasses.field(default_factory=list)

    def is_valid(
        self,
        *,
        size: int | None = None,
        subset_tags: Iterable[str] | None = None,
        superset_tags: Iterable[str] | None = None,
    ) -> bool:
        """Check if set of contstraints is valid for this policy.

        Note:
            All arguments are optional keyword arguments that default to
            `None`. If left as the default, that constraint will not be
            checked against the policy.

        Args:
            size: Object size.
            subset_tags: Set of tags that must be a subset
                of the Policy's `subset_tags` to be valid.
            superset_tags: Set of tags that must be a superset
                of the Policy's `superset_tags` to be valid.

        Returns:
            If the provided constraints are valid for the policy.
        """
        if size is not None and (size < self.min_size or size > self.max_size):
            return False
        if subset_tags is not None and not set(subset_tags).issubset(
            self.subset_tags,
        ):
            return False
        if superset_tags is not None and not set(superset_tags).issuperset(
            self.superset_tags,
        ):
            return False
        return True

    def as_dict(self) -> PolicyDict:
        """Convert the Policy to a JSON compatible dict.

        Usage:
            >>> policy = Policy(...)
            >>> policy_dict = policy.as_dict()
            >>> Policy(**policy_dict) == policy
            True
        """
        # We could use dataclasses.asdict(self) but this gives us the benefit
        # of typing on the return dict.
        return PolicyDict(
            priority=self.priority,
            min_size=self.min_size,
            max_size=self.max_size,
            subset_tags=self.subset_tags,
            superset_tags=self.superset_tags,
        )


class _StorePolicyArgs(TypedDict):
    """Internal dictionary used by MultiStore.kwargs for reconstrction."""

    name: str
    kind: type[Store[Any]]
    kwargs: dict[str, Any]
    policy: Policy


class _StorePolicy(NamedTuple):
    store: Store[Any]
    policy: Policy


class MultiStoreKey(NamedTuple):
    """Key to objects in a MultiStore."""

    store_name: str
    store_key: NamedTuple


class MultiStore(Store[MultiStoreKey]):
    """Policy based manager for a collection of [`Store`][proxystore.store.base.Store].

    Note:
        This store does not implement
        [`get_bytes()`][proxystore.store.base.Store.get_bytes] or
        [`set_bytes()`][proxystore.store.base.Store.set_bytes] because
        `MultiStore.get()` and `MultiStore.set()` forward operations to the
        corresponding store.

    Warning:
        `MultiStore.close()` will call
        [`Store.close()`][proxystore.store.base.Store.close]
        on all the stores managed by the instance and unregister them.

    Args:
        name: Name of this store instance.
        stores: Mapping of stores (either
            [`Store`][proxystore.store.base.Store] instances or string
            names of registered stores) to the corresponding
            [`Policy`][proxystore.store.multi.Policy]. If
            [`Store`][proxystore.store.base.Store] instances are passed,
            the instances will be registered.
        cache_size: Size of LRU cache (in # of objects). If 0,
            the cache is disabled. The cache is local to the Python process.
        stats: collect stats on store operations.
    """

    def __init__(
        self,
        name: str,
        *,
        stores: dict[str, Policy]
        | dict[Store[Any], Policy]
        | Sequence[_StorePolicyArgs],
        cache_size: int = 0,
        stats: bool = False,
    ) -> None:
        # Cache and stats are controlled by the wrapped Stores.
        super().__init__(
            name,
            cache_size=0,
            stats=False,
            # We override the kwargs property so no need to pass here
            kwargs={},
        )

        self._stores: dict[str, _StorePolicy] = {}

        if isinstance(stores, dict):
            for store, policy in stores.items():
                if isinstance(store, str):
                    possible_store = get_store(store)
                    if possible_store is None:
                        raise RuntimeError(
                            f'A store named "{store}" is not registered.',
                        )
                    actual_store = possible_store
                else:
                    actual_store = store

                self._stores[actual_store.name] = _StorePolicy(
                    store=actual_store,
                    policy=policy,
                )
        elif isinstance(stores, Sequence):
            for store_args in stores:
                possible_store = get_store(store_args['name'])
                if possible_store is None:
                    actual_store = store_args['kind'](
                        store_args['name'],
                        **store_args['kwargs'],
                    )
                else:
                    actual_store = possible_store
                policy = store_args['policy']

                self._stores[actual_store.name] = _StorePolicy(
                    store=actual_store,
                    policy=policy,
                )
        else:
            raise AssertionError('Unreachable.')

        # Register so multiple instances of `MultiStore` in a process
        # use the same underlying stores for caching/efficiency.
        for store, _ in self._stores.values():
            register_store(store, exist_ok=True)

        self._stores_by_priority = sorted(
            self._stores,
            key=lambda name: self._stores[name].policy.priority,
            reverse=True,
        )

    @property
    def kwargs(self) -> dict[str, Any]:
        _kwargs = super().kwargs

        store_policy_args: list[_StorePolicyArgs] = []
        for name, (store, policy) in self._stores.items():
            store_policy_args.append(
                _StorePolicyArgs(
                    name=name,
                    kind=type(store),
                    kwargs=store.kwargs,
                    policy=policy,
                ),
            )

        _kwargs['stores'] = store_policy_args
        return _kwargs

    def close(self) -> None:
        for store, _ in self._stores.values():
            store.close()
            unregister_store(store.name)

    def evict(self, key: MultiStoreKey) -> None:
        store = self._stores[key.store_name].store
        store.evict(key.store_key)

    def exists(self, key: MultiStoreKey) -> bool:
        store = self._stores[key.store_name].store
        return store.exists(key.store_key)

    def get(
        self,
        key: MultiStoreKey,
        *,
        deserializer: DeserializerT | None = None,
        default: object | None = None,
    ) -> Any | None:
        store = self._stores[key.store_name].store
        return store.get(
            key.store_key,
            deserializer=deserializer,
            default=default,
        )

    def get_bytes(self, key: MultiStoreKey) -> bytes | None:
        raise NotImplementedError('MultiStore does not implement get_bytes.')

    def proxy(
        self,
        obj: T,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
        **kwargs: Any,
    ) -> Proxy[T]:
        """Create a proxy that will resolve to an object in the store.

        Warning:
            If the factory requires reinstantiating the store to correctly
            resolve the object, the factory should reinstantiate the store
            with the same arguments used to instantiate the store that
            created the proxy/factory. I.e. the
            [`proxy()`][proxystore.store.multi.MultiStore.proxy] method
            should pass any arguments given to
            [`Store`][proxystore.store.base.Store]
            along to the factory so the factory can correctly recreate the
            store if the factory is resolved in a different Python process.

        Args:
            obj: Object to place in store and return proxy for.
            serializer: optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer ([`deserialize()`][proxystore.serialize.deserialize])
                will be used.
            subset_tags: Iterable of tags that must be a subset
                of a store policy's `subset_tags` to match that store.
            superset_tags: Iterable of tags that must be a
                superset of a store policy's `superset_tags` to match that
                store.
            kwargs: Additional arguments to pass to the factory.

        Returns:
            A proxy of the object.
        """
        key = self.set(
            obj,
            serializer=serializer,
            subset_tags=subset_tags,
            superset_tags=superset_tags,
        )
        return self.proxy_from_key(key, deserializer=deserializer, **kwargs)

    def proxy_batch(
        self,
        objs: Sequence[T],
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
        **kwargs: Any,
    ) -> list[Proxy[T]]:
        """Create proxies for batch of objects in the store.

        See [`Store.proxy()`][proxystore.store.base.Store.proxy] for more
        details.

        Args:
            objs (Sequence[object]): Objects to place in store and return
                proxies for.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            subset_tags: Iterable of tags that must be a subset
                of a store policy's `subset_tags` to match that store.
            superset_tags: Iterable of tags that must be a
                superset of a store policy's `superset_tags` to match that
                store.
            kwargs: Additional arguments to pass to the Factory.

        Returns:
            A list of proxies of the objects.
        """
        keys = self.set_batch(
            objs,
            serializer=serializer,
            subset_tags=subset_tags,
            superset_tags=superset_tags,
        )
        return [
            self.proxy_from_key(key, deserializer=deserializer, **kwargs)
            for key in keys
        ]

    def locked_proxy(
        self,
        obj: T,
        serializer: SerializerT | None = None,
        deserializer: DeserializerT | None = None,
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
        **kwargs: Any,
    ) -> ProxyLocker[T]:
        """Create a proxy locker that will prevent resolution.

        Args:
            obj: Object to place in store and create proxy of.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            deserializer: Optional callable used by the factory
                to deserialize the byte string. If `None`, the default
                deserializer
                ([`deserialize()`][proxystore.serialize.deserialize]) will be
                used.
            subset_tags: Iterable of tags that must be a subset
                of a store policy's `subset_tags` to match that store.
            superset_tags: Iterable of tags that must be a
                superset of a store policy's `superset_tags` to match that
                store.
            kwargs: Additional arguments to pass to the Factory.

        Returns:
            A proxy wrapped in a [`ProxyLocker`][proxystore.proxy.ProxyLocker].
        """
        return ProxyLocker(
            self.proxy(
                obj,
                serializer=serializer,
                deserializer=deserializer,
                subset_tags=subset_tags,
                superset_tags=superset_tags,
                **kwargs,
            ),
        )

    def set(
        self,
        obj: Any,
        *,
        serializer: SerializerT | None = None,
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
    ) -> MultiStoreKey:
        """Set key-object pair in store.

        Args:
            obj: Object to be placed in the store.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            subset_tags: Iterable of tags that must be a subset
                of a store policy's `subset_tags` to match that store.
            superset_tags: Iterable of tags that must be a
                superset of a store policy's `superset_tags` to match that
                store.

        Returns:
            A key that can be used to retrieve the object.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        if serializer is not None:
            obj = serializer(obj)
        else:
            obj = default_serializer(obj)

        if not isinstance(obj, bytes):
            raise TypeError('Serializer must produce bytes.')

        for store_name in self._stores_by_priority:
            store, policy = self._stores[store_name]
            if policy.is_valid(
                size=len(obj),
                subset_tags=subset_tags,
                superset_tags=superset_tags,
            ):
                # We already serialized object so pass identity
                # function to avoid duplicate serialization
                key = store.set(obj, serializer=_identity)
                return MultiStoreKey(
                    store_name=store.name,
                    store_key=key,
                )
        else:
            raise ValueError(
                'No store policy was suitable for the constraints: '
                f'subset_tags={subset_tags}, superset_tags={superset_tags}.',
            )

    def set_batch(
        self,
        objs: Sequence[Any],
        *,
        serializer: SerializerT | None = None,
        subset_tags: Iterable[str] = (),
        superset_tags: Iterable[str] = (),
    ) -> list[MultiStoreKey]:
        """Set objects in store.

        Args:
            objs: An iterable of objects to be placed in the store.
            serializer: Optional callable which serializes the
                object. If `None`, the default serializer
                ([`serialize()`][proxystore.serialize.serialize]) will be used.
            subset_tags: Iterable of tags that must be a subset
                of a store policy's `subset_tags` to match that store.
            superset_tags: Iterable of tags that must be a
                superset of a store policy's `superset_tags` to match that
                store.

        Returns:
            List of keys that can be used to retrieve the objects.

        Raises:
            TypeError: If the output of `serializer` is not bytes.
        """
        return [
            self.set(
                obj,
                serializer=serializer,
                subset_tags=subset_tags,
                superset_tags=superset_tags,
            )
            for obj in objs
        ]

    def set_bytes(self, key: MultiStoreKey, data: bytes) -> None:
        raise NotImplementedError('MultiStore does not implement set_bytes.')

"""Base Store Abstract Class."""
from __future__ import annotations

import copy
import logging
from abc import ABCMeta
from abc import abstractmethod
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Sequence

import proxystore as ps
from proxystore.factory import Factory
from proxystore.store.exceptions import ProxyResolveMissingKey
from proxystore.store.stats import FunctionEventStats
from proxystore.store.stats import STORE_METHOD_KEY_IS_RESULT
from proxystore.store.stats import TimeStats

_default_pool = ThreadPoolExecutor()
logger = logging.getLogger(__name__)


class StoreFactory(Factory):
    """Base Factory for Stores.

    Adds support for asynchronously retrieving objects from a
    :class:`Store <.Store>`.

    The factory takes the `store_type` and `store_kwargs` parameters that are
    used to reinitialize the store if the factory is sent to a remote
    process where the store has not already been initialized.
    """

    def __init__(
        self,
        key: str,
        store_type: type[Store],
        store_name: str,
        store_kwargs: dict[str, Any] | None = None,
        *,
        evict: bool = False,
        **kwargs: Any,
    ) -> None:
        """Init StoreFactory.

        Args:
            key (str): key corresponding to object in store.
            store_type (Store): type of store this factory will resolve an
                object from.
            store_name (str): name of store
            store_kwargs (dict): optional keyword arguments used to
                reinitialize store.
            evict (bool): If True, evict the object from the store once
                :func:`resolve()` is called (default: False).
            kwargs (dict): additional keyword arguments to set as attributes
                of the factory.
        """
        self.key = key
        self.store_type = store_type
        self.store_name = store_name
        self.store_kwargs = {} if store_kwargs is None else store_kwargs
        self.evict = evict
        self.kwargs = kwargs

        self.__dict__.update(kwargs)

        # The following are not included when a factory is serialized
        # because they are specific to that instance of the factory
        self._obj_future: Future[Any] | None = None
        self.stats: FunctionEventStats | None = None
        if 'stats' in self.store_kwargs and self.store_kwargs['stats'] is True:
            self.stats = FunctionEventStats()
            # Monkeypatch methods with wrappers to track their stats
            setattr(  # noqa: B010
                self,
                'resolve',
                self.stats.wrap(self.resolve, preset_key=self.key),
            )
            setattr(  # noqa: B010
                self,
                'resolve_async',
                self.stats.wrap(self.resolve_async, preset_key=self.key),
            )

    def __getnewargs_ex__(
        self,
    ) -> tuple[tuple[str, type[Store], str, dict[str, Any]], dict[str, Any]]:
        """Pickle without possible futures."""
        return (
            self.key,
            self.store_type,
            self.store_name,
            self.store_kwargs,
        ), {'evict': self.evict, **self.kwargs}

    def _get_value(self) -> Any:
        """Get the value associated with the key from the store.

        This method is intended to be overridden by child classes that
        have special keyword arguments to pass to their corresponding
        Store types.
        """
        store = self.get_store()
        obj = store.get(self.key)

        if obj is None:
            raise ProxyResolveMissingKey(
                self.key,
                self.store_type,
                self.store_name,
            )

        if self.evict:
            store.evict(self.key)

        return obj

    def _should_resolve_async(self) -> bool:
        """Check if it makes sense to do asynchronous resolution.

        This method is intended to be overridden by child classes that may
        have different conditions on which asynchronous resolution makes
        sense.
        """
        return not self.get_store().is_cached(self.key)

    def get_store(self) -> Store:
        """Get store and reinitialize if necessary.

        Raises:
            ValueError:
                if the type of the returned store does not match the expected
                store type passed to the factory constructor.
        """
        store = ps.store.get_store(self.store_name)
        if store is None:
            store = ps.store.init_store(
                self.store_type,
                self.store_name,
                **self.store_kwargs,
            )

        if not isinstance(store, self.store_type):
            raise ValueError(
                f'store_name={self.store_name} passed to '
                f'{type(self).__name__} does not correspond to store of '
                f'type {self.store_type.__name__}',
            )

        return store

    def resolve(self) -> Any:
        """Get object associated with key from store.

        Raises:
            ProxyResolveMissingKey:
                if the key associated with this factory does not exist
                in the store.
        """
        if self._obj_future is not None:
            obj = self._obj_future.result()
            self._obj_future = None
            return obj

        return self._get_value()

    def resolve_async(self) -> None:
        """Asynchronously get object associated with key from store."""
        if self._should_resolve_async():
            self._obj_future = _default_pool.submit(self._get_value)


class Store(metaclass=ABCMeta):
    """Abstraction of a key-value store."""

    def __init__(self, name: str, *, stats: bool = False) -> None:
        """Init Store.

        Args:
            name (str): name of the store instance.
            stats (bool): collect stats on store operations (default: False)
        """
        self.name = name

        self._stats: FunctionEventStats | None = None
        if stats:
            self._stats = FunctionEventStats()
            # Monkeypatch methods with wrappers to track their stats
            for attr in dir(self):
                if (
                    callable(getattr(self, attr))
                    and not attr.startswith('_')
                    and attr in STORE_METHOD_KEY_IS_RESULT
                ):
                    method = getattr(self, attr)
                    # For most method, the key is the first arg which wrap()
                    # expects by default, but there are a couple where the
                    # key is passed as a kwarg
                    wrapped = self._stats.wrap(
                        method,
                        key_is_result=STORE_METHOD_KEY_IS_RESULT[attr],
                    )
                    setattr(self, attr, wrapped)

        logger.debug(f'Initialized {self}')

    @property
    def has_stats(self) -> bool:
        """Whether the store keeps track of performance stats."""
        return self._stats is not None

    def __repr__(self) -> str:
        """Represent Store instance as string."""
        s = f'{ps.utils.fullname(self.__class__)}('
        attributes = [
            f'{key}={value}'
            for key, value in self.__dict__.items()
            if not key.startswith('_') and not callable(value)
        ]
        attributes.sort()
        s += ', '.join(attributes)
        s += ')'
        return s

    @property
    def kwargs(self) -> dict[str, Any]:
        """Get kwargs for store instance."""
        return self._kwargs()

    def _kwargs(
        self,
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Helper for handling inheritance with kwargs property.

        Args:
            kwargs (optional, dict): dict to use as return object. If None,
                a new dict will be created.
        """
        if kwargs is None:
            kwargs = {}
        kwargs.update({'stats': self._stats is not None})
        return kwargs

    def cleanup(self) -> None:
        """Cleanup any objects associated with the store.

        Many :class:`Store <.Store>` types do not have any objects that
        requiring cleaning up so this method is simply a no-op.

        Warning:
            This method should only be called at the end of the program
            when the store will no longer be used, for example once all
            proxies have been resolved.
        """
        pass

    def create_key(self, obj: Any) -> str:
        """Create key for the object.

        Args:
            obj: object to be placed in store.

        Returns:
            key (str)
        """
        return ps.utils.create_key(obj)

    @abstractmethod
    def evict(self, key: str) -> None:
        """Evict object associated with key.

        Args:
            key (str): key corresponding to object in store to evict.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists.

        Args:
            key (str): key to check.

        Returns:
            `bool`
        """
        raise NotImplementedError

    @abstractmethod
    def get(
        self,
        key: str,
        *,
        strict: bool = False,
        default: Any = None,
    ) -> object | None:
        """Return object associated with key.

        Args:
            key (str): key corresponding to object.
            strict (bool): guarantee returned object is the most recent
                version (default: False).
            default: optionally provide value to be returned if an object
                associated with the key does not exist (default: None).

        Returns:
            object associated with key or `default` if key does not exist.
        """
        raise NotImplementedError

    @abstractmethod
    def is_cached(self, key: str, *, strict: bool = False) -> bool:
        """Check if object is cached locally.

        Args:
            key (str): key corresponding to object.
            strict (bool): guarantee object in cache is most recent version
                (default: False).

        Returns:
            `bool`
        """
        raise NotImplementedError

    @abstractmethod
    def proxy(
        self,
        obj: Any | None = None,
        *,
        key: str | None = None,
        factory: type[StoreFactory] = StoreFactory,
        **kwargs: Any,
    ) -> ps.proxy.Proxy:
        """Create a proxy that will resolve to an object in the store.

        Warning:
            If the factory requires reinstantiating the store to correctly
            resolve the object, the factory should reinstantiate the store
            with the same arguments used to instantiate the store that
            created the proxy/factory. I.e. the :func:`proxy()` function
            should pass any arguments given to :func:`Store.__init__()`
            along to the factory so the factory can correctly recreate the
            store if the factory is resolved in a different Python process.

        Args:
            obj (object): object to place in store and return proxy for.
                If an object is not provided, a key must be provided that
                corresponds to an object already in the store
                (default: None).
            key (str): optional key to associate with `obj` in the store.
                If not provided, a key will be generated (default: None).
            factory (StoreFactory): factory class that will be instantiated
                and passed to the proxy. The factory class should be able
                to correctly resolve the object from this store (default:
                :any:`StoreFactory <proxystore.store.base.StoreFactory>`).
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `key` and `obj` are both `None`.
            ValueError:
                if `obj` is None and `key` does not exist in the store.
        """
        raise NotImplementedError

    @abstractmethod
    def proxy_batch(
        self,
        objs: Sequence[Any] | None = None,
        *,
        keys: Sequence[str] | None = None,
        factory: StoreFactory | None = None,
        **kwargs: Any,
    ) -> list[ps.proxy.Proxy]:
        """Create proxies for batch of objects in the store.

        See :any:`proxy() <proxystore.store.base.Store.proxy>` for more
        details.

        Args:
            objs (Sequence[object]): objects to place in store and return
                proxies for. If an iterable of objects is not provided, an
                iterable of keys must be provided that correspond to objects
                already in the store (default: None).
            keys (Sequence[str]): optional keys to associate with `objs` in the
                store. If not provided, keys will be generated (default: None).
            factory (StoreFactory): Optional factory class that will be
                instantiated and passed to the proxies. The factory class
                should be able to correctly resolve an object from this store.
                Defaults to None so the default of :func:`proxy()` is used.
            kwargs (dict): additional arguments to pass to the Factory.

        Returns:
            List of :any:`Proxy <proxystore.proxy.Proxy>`

        Raises:
            ValueError:
                if `keys` and `objs` are both `None`.
            ValueError:
                if `objs` is None and `keys` does not exist in the store.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, obj: Any, *, key: str | None = None) -> str:
        """Set key-object pair in store.

        Args:
            obj (object): object to be placed in the store.
            key (str, optional): key to use with the object. If the key is not
                provided, one will be created.

        Returns:
            key (str). Note that some implementations of a store may return
            a key different from the provided key.
        """
        raise NotImplementedError

    @abstractmethod
    def set_batch(
        self,
        objs: Sequence[Any],
        *,
        keys: Sequence[str | None] | None = None,
    ) -> list[str]:
        """Set objects in store.

        Args:
            objs (Sequence[object]): iterable of objects to be placed in the
                store.
            keys (Sequence[str], optional): keys to use with the objects.
                If the keys are not provided, keys will be created.

        Returns:
            List of keys (str). Note that some implementations of a store may
            return keys different from the provided keys.

        Raises:
            ValueError:
                if :code:`keys is not None` and :code:`len(objs) != len(keys)`.
        """
        raise NotImplementedError

    def stats(
        self,
        key_or_proxy: str | ps.proxy.Proxy,
    ) -> dict[str, TimeStats]:
        """Get stats on the store.

        Args:
            key_or_proxy (str, Proxy): key to get stats for or a proxy to
                extract the key from.

        Returns:
            dict with keys corresponding to method names and values which are
            :class:`TimeStats <proxystore.store.stats.TimeStats>` instances
            with the statistics for calls to the corresponding method with the
            specified key.

            Example:

            .. code-block:: python

               {
                   "get": TimeStats(
                       calls=32,
                       avg_time_ms=0.0123,
                       min_time_ms=0.0012,
                       max_time_ms=0.1234,
                   ),
                   "set": TimeStats(...),
                   "evict": TimeStats(...),
                   ...
               }

        Raises:
            ValueError:
                if `self` was initialized with :code:`stats=False`.
        """
        if self._stats is None:
            raise ValueError(
                'Stats are not being tracked because this store was '
                'initialized with stats=False.',
            )
        stats = {}
        if isinstance(key_or_proxy, ps.proxy.Proxy):
            key = ps.proxy.get_key(key_or_proxy)
            # Merge stats from the proxy into self
            if hasattr(key_or_proxy.__factory__, 'stats'):
                proxy_stats = key_or_proxy.__factory__.stats
                if proxy_stats is not None:
                    for event in proxy_stats:
                        stats[event.function] = copy.copy(proxy_stats[event])
        else:
            key = key_or_proxy

        for event in self._stats:
            if event.key == key:
                stats[event.function] = copy.copy(self._stats[event])
        return stats

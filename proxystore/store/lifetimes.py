"""Lifetime managers for objects in shared stores.

Learn more about managing object lifetimes in the
[Object Lifetimes](../../guides/object-lifetimes.md) guide.
"""

from __future__ import annotations

import atexit
import functools
import logging
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import Concatenate
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import Concatenate
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import get_factory
from proxystore.proxy import Proxy
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.factory import StoreFactory
from proxystore.store.types import ConnectorKeyT

if TYPE_CHECKING:
    from proxystore.store.base import Store

logger = logging.getLogger(__name__)


@runtime_checkable
class Lifetime(Protocol):
    """Lifetime protocol."""

    def add_key(
        self,
        *keys: ConnectorKeyT,
        store: Store[Any] | None = None,
    ) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All keys should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            keys: One or more keys of objects to associate with this lifetime.
            store: Optional [`Store`][proxystore.store.base.Store] that `keys`
                belongs to.
        """
        ...

    def add_proxy(self, *proxies: Proxy[Any]) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All proxies should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            proxies: One or more proxies of objects to associate with this
                lifetime.

        Raises:
            ProxyStoreFactoryError: If the proxy's factory is not an instance
                of [`StoreFactory`][proxystore.store.base.StoreFactory].
        """
        ...

    def close(self, *, close_stores: bool = False) -> None:
        """End the lifetime and evict all associated objects.

        Args:
            close_stores: Close any [`Store`][proxystore.store.base.Store]
                store instances associated with the lifetime.
        """
        ...

    def done(self) -> bool:
        """Check if lifetime has ended."""
        ...


P = ParamSpec('P')
T = TypeVar('T')
LifetimeT = TypeVar('LifetimeT', bound=Lifetime)


def _error_if_done(
    method: Callable[Concatenate[LifetimeT, P], T],
) -> Callable[Concatenate[LifetimeT, P], T]:
    @functools.wraps(method)
    def _check(self: LifetimeT, *args: P.args, **kwargs: P.kwargs) -> T:
        if self.done():
            raise RuntimeError('Lifetime has ended. Cannot use this method.')
        return method(self, *args, **kwargs)

    return _check


class ContextLifetime:
    """Basic lifetime manager.

    Object lifetime manager with context manager support.

    Example:
        ```python
        from proxystore.store.base import Store
        from proxystore.store.lifetimes import ContextLifetime

        store = Store(...)

        with ContextLifetime(store) as lifetime:
            # Objects in the store can be associated with this lifetime.
            key = store.put('value', lifetime=lifetime)
            proxy = store.proxy('value', lifetime=lifetime)

        # Objects associated with the lifetime are evicted once the
        # lifetime ends.
        assert not store.exists(key)

        store.close()
        ```

    Args:
        store: [`Store`][proxystore.store.base.Store] instance use to create
            the objects associated with this lifetime and that will be used
            to evict them when the lifetime has ended.
        name: Specify a name for this lifetime used in logging. Otherwise,
            a unique ID will be generated.
    """

    def __init__(
        self,
        store: Store[Any],
        *,
        name: str | None = None,
    ) -> None:
        self.store = store
        self.name = name if name is not None else str(uuid.uuid4())
        self._done = False
        self._keys: set[ConnectorKeyT] = set()

        logger.info(f'Initialized lifetime manager (name={self.name})')

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
        return f'Lifetime(name={self.name}, store={self.store!r})'

    @_error_if_done
    def add_key(
        self,
        *keys: ConnectorKeyT,
        store: Store[Any] | None = None,
    ) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All keys should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            keys: One or more keys of objects to associate with this lifetime.
            store: Optional [`Store`][proxystore.store.base.Store] that `keys`
                belongs to. Ignored by this implementation.

        Raises:
            RuntimeError: If this lifetime has ended.
        """
        self._keys.update(keys)
        logger.debug(
            f'Added keys to lifetime manager (name={self.name}): '
            f'{", ".join(repr(key) for key in keys)}',
        )

    @_error_if_done
    def add_proxy(self, *proxies: Proxy[Any]) -> None:
        """Associate a new object with the lifetime.

        Warning:
            All proxies should have been created by the same
            [`Store`][proxystore.store.base.Store] that this lifetime was
            initialized with.

        Args:
            proxies: One or more proxies of objects to associate with this
                lifetime.

        Raises:
            ProxyStoreFactoryError: If the proxy's factory is not an instance
                of [`StoreFactory`][proxystore.store.base.StoreFactory].
            RuntimeError: If this lifetime has ended.
        """
        keys: list[ConnectorKeyT] = []
        for proxy in proxies:
            factory = get_factory(proxy)
            if isinstance(factory, StoreFactory):
                keys.append(factory.key)
            else:
                raise ProxyStoreFactoryError(
                    'The proxy must contain a factory with type '
                    f'{StoreFactory.__name__}. {type(factory).__name__} '
                    'is not supported.',
                )
        self.add_key(*keys)

    def close(self, *, close_stores: bool = False) -> None:
        """End the lifetime and evict all associated objects.

        Args:
            close_stores: Close any [`Store`][proxystore.store.base.Store]
                store instances associated with the lifetime.
        """
        if self.done():
            return

        for key in self._keys:
            self.store.evict(key)
        self._done = True
        logger.info(
            f'Closed lifetime manager and evicted {len(self._keys)} '
            f'associated objects (name={self.name})',
        )
        self._keys.clear()

        if close_stores:
            self.store.close()

    def done(self) -> bool:
        """Check if lifetime has ended."""
        return self._done


class LeaseLifetime(ContextLifetime):
    """Time-based lease lifetime manager.

    Example:
        ```python
        from proxystore.store.base import Store
        from proxystore.store.lifetimes import LeaseLifetime

        with Store(...) as store:
            # Create a new lifetime with a current lease of ten seconds.
            lifetime = LeaseLifetime(store, expiry=10)

            # Objects in the store can be associated with this lifetime.
            key = store.put('value', lifetime=lifetime)
            proxy = store.proxy('value', lifetime=lifetime)

            # Extend the lease by another five seconds.
            lifetime.extend(5)

            time.sleep(15)

            # Lease has expired so the lifetime has ended.
            assert lifetime.done()
            assert not store.exists(key)
        ```

    Args:
        store: [`Store`][proxystore.store.base.Store] instance use to create
            the objects associated with this lifetime and that will be used
            to evict them when the lifetime has ended.
        expiry: Initial expiry time of the lease. Can either be a
            [`datetime`][datetime.datetime], [`timedelta`][datetime.timedelta],
            or float value specifying the number of seconds before expiring.
        name: Specify a name for this lifetime used in logging. Otherwise,
            a unique ID will be generated.
    """

    def __init__(
        self,
        store: Store[Any],
        expiry: datetime | timedelta | float,
        *,
        name: str | None = None,
    ) -> None:
        if isinstance(expiry, datetime):
            self._expiry = expiry.timestamp()
        elif isinstance(expiry, timedelta):
            self._expiry = time.time() + expiry.total_seconds()
        elif isinstance(expiry, (int, float)):
            self._expiry = time.time() + expiry
        else:
            raise AssertionError('Unreachable.')

        super().__init__(store, name=name)

        self._timer: threading.Timer | None = None
        self._start_timer()

    def _timer_callback(self) -> None:
        if time.time() >= self._expiry:
            self.close()
        else:  # pragma: no cover
            # Excluded from coverage because some MacOS GitHub actions runners
            # are slow enough that the timer always ends after the expiry
            # in the tests cases. We could make the expiry very long, but
            # that would just slow down the test suite.
            self._start_timer()

    def _start_timer(self) -> None:
        if self._timer is not None:  # pragma: no cover
            # Excluded from coverage for the same reason as in
            # _timer_callback().
            self._timer.cancel()
        interval = max(0, self._expiry - time.time())
        self._timer = threading.Timer(interval, self._timer_callback)
        self._timer.start()

    def close(self, *, close_stores: bool = False) -> None:
        """End the lifetime and evict all associated objects.

        This can be called before the specified expiry time to end the
        lifetime early.

        Args:
            close_stores: Close any [`Store`][proxystore.store.base.Store]
                store instances associated with the lifetime.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        super().close(close_stores=close_stores)

    @_error_if_done
    def extend(self, expiry: datetime | timedelta | float) -> None:
        """Extend the expiry of the lifetime lease.

        Args:
            expiry: Extends the current expiry if the value is
                [`timedelta`][datetime.timedelta] or float value specifying
                seconds. If a [`datetime`][datetime.datetime], updates the
                expiry to the specified timestamp even if the new time
                is less than the current expiry.
        """
        if isinstance(expiry, datetime):
            self._expiry = expiry.timestamp()
        elif isinstance(expiry, timedelta):
            self._expiry += expiry.total_seconds()
        elif isinstance(expiry, (int, float)):
            self._expiry += expiry
        else:
            raise AssertionError('Unreachable.')


class StaticLifetime:
    """Static lifetime manager.

    Keeps associated objects alive for the remainder of the program.

    Note:
        This is a singleton class.

    Warning:
        This class registers an [atexit][atexit] handler which will close
        the lifetime at the end of the program, evicting all objects associated
        with the lifetime. Therefore, [`Store`][proxystore.store.base.Store]
        instances used to created objects associated with the static lifetime
        **should not** be closed prior to program exit. The handler will close
        all of these stores. It is possible to call `StaticLifetime().close()`
        manually, after which it is safe to also close the stores.

    Example:
        ```python linenums="1" title="Static Lifetime"
        from proxystore.connectors.local import LocalConnector
        from proxystore.store import Store
        from proxystore.store.lifetimes import StaticLifetime

        store = Store('default', LocalConnector(), register=True)  # (1)!

        key = store.put('value', lifetime=StaticLifetime())  # (2)!
        proxy = store.proxy('value', lifetime=StaticLifetime())  # (3)!
        ```

        1. The atexit handler will call `store.close()` at the end of the
           program. Setting `register=True` is recommended to prevent another
           instance being created internally when a proxy is resolved.
        3. The object associated with `key` will be evicted at the end of
           the program.
        4. The object associated with `proxy` will be evicted at the end of
           the program.
    """

    _instance: StaticLifetime | None = None
    name = 'static'

    def __new__(cls) -> StaticLifetime:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized:
            self._done = False
            self._keys: dict[Store[Any], set[ConnectorKeyT]] = defaultdict(set)
            self._callback = register_lifetime_atexit(self, close_stores=True)
            self._initialized = True

    @_error_if_done
    def add_key(
        self,
        *keys: ConnectorKeyT,
        store: Store[Any] | None = None,
    ) -> None:
        """Associate a new object with the lifetime.

        Args:
            keys: One or more keys of objects to associate with this lifetime.
            store: [`Store`][proxystore.store.base.Store] that `keys`
                belongs to. Required by this implementation.

        Raises:
            RuntimeError: If this lifetime has ended.
            ValueError: If `store` is `None`.
        """
        if store is None:
            raise ValueError(
                f'The {self.__class__.__name__} requires the store parameter.',
            )
        self._keys[store].update(keys)
        logger.debug(
            f'Added keys to lifetime manager (name={self.name}): '
            f'{", ".join(repr(key) for key in keys)}',
        )

    @_error_if_done
    def add_proxy(self, *proxies: Proxy[Any]) -> None:
        """Associate a new object with the lifetime.

        Warning:
            This method will initialized new
            [`Store`][proxystore.store.base.Store] instances if the stores
            which were used to create the input proxies have not been
            registered by setting the `register` flag or by calling
            [`register_store()`][proxystore.store.register_store].

        Args:
            proxies: One or more proxies of objects to associate with this
                lifetime.

        Raises:
            ProxyStoreFactoryError: If the proxy's factory is not an instance
                of [`StoreFactory`][proxystore.store.base.StoreFactory].
            RuntimeError: If this lifetime has ended.
        """
        for proxy in proxies:
            factory = get_factory(proxy)
            if isinstance(factory, StoreFactory):
                self.add_key(factory.key, store=factory.get_store())
            else:
                raise ProxyStoreFactoryError(
                    'The proxy must contain a factory with type '
                    f'{StoreFactory.__name__}. {type(factory).__name__} '
                    'is not supported.',
                )

    def close(self, *, close_stores: bool = False) -> None:
        """End the lifetime and evict all associated objects.

        Warning:
            Because this class is a singleton this operation can only
            be performed once.

        Args:
            close_stores: Close any [`Store`][proxystore.store.base.Store]
                store instances associated with the lifetime.
        """
        if self.done():
            return

        count = 0
        for store, keys in self._keys.items():
            for key in keys:
                store.evict(key)
                count += 1

            if close_stores:
                store.close()

        atexit.unregister(self._callback)

        self._done = True
        logger.info(
            f'Closed lifetime manager and evicted {count} '
            f'associated objects (name={self.name})',
        )
        self._keys.clear()

    def done(self) -> bool:
        """Check if lifetime has ended."""
        return self._done


def register_lifetime_atexit(
    lifetime: Lifetime,
    close_stores: bool = True,
) -> Callable[[], None]:
    """Register atexit callback to cleanup the lifetime.

    Registers an atexit callback which will close the lifetime on normal
    program exit and optionally close the associated store as well.

    Tip:
        Do not close the [`Store`][proxystore.store.base.Store] associated
        with the lifetime when registering an atexit callback. Using a
        [`Store`][proxystore.store.base.Store] after closing it is undefined
        behaviour. Rather, let the callback handle closing after it is
        safe to do so.

    Warning:
        Callbacks are not guaranteed to be called in all cases. See the
        [`atexit`][atexit] docs for more details.

    Args:
        lifetime: Lifetime to be closed at exit.
        close_stores: Close any [`Store`][proxystore.store.base.Store]
            instances associated with the lifetime.

    Returns:
        The registered callback function which can be used with \
        [`atexit.unregister()`][atexit.unregister] if needed.
    """

    def _lifetime_atexit_callback() -> None:
        lifetime.close(close_stores=close_stores)

    atexit.register(_lifetime_atexit_callback)
    logger.debug(
        f'Registered atexit callback for {lifetime!r}',
    )
    return _lifetime_atexit_callback

from __future__ import annotations

import atexit
import time
from datetime import datetime
from datetime import timedelta
from typing import Any

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.lifetimes import ContextLifetime
from proxystore.store.lifetimes import LeaseLifetime
from proxystore.store.lifetimes import Lifetime
from proxystore.store.lifetimes import register_lifetime_atexit


def test_context_lifetime_protocol(store: Store[LocalConnector]) -> None:
    lifetime = ContextLifetime(store)
    assert isinstance(lifetime, Lifetime)
    lifetime.close()


def test_context_lifetime_cleanup(store: Store[LocalConnector]) -> None:
    key1 = store.put('value1')
    key2 = store.put('value2')
    key3 = store.put('value3')
    key4 = store.put('value4')
    proxy1: Proxy[str] = store.proxy_from_key(key3)
    proxy2: Proxy[str] = store.proxy_from_key(key4)

    with ContextLifetime(store) as lifetime:
        assert not lifetime.done()

        lifetime.add_key(key1, key2)
        lifetime.add_proxy(proxy1, proxy2)

    assert lifetime.done()

    assert not store.exists(key1)
    assert not store.exists(key2)
    assert not store.exists(key3)
    assert not store.exists(key4)


def test_context_lifetime_close_idempotency(
    store: Store[LocalConnector],
) -> None:
    lifetime = ContextLifetime(store)
    lifetime.close()
    lifetime.close()


def test_context_lifetime_add_bad_proxy(store: Store[LocalConnector]) -> None:
    proxy: Proxy[list[Any]] = Proxy(list)

    with ContextLifetime(store) as lifetime:
        with pytest.raises(ProxyStoreFactoryError):
            lifetime.add_proxy(proxy)


def test_context_lifetime_error_if_done(store: Store[LocalConnector]) -> None:
    key = store.put('value')
    proxy: Proxy[str] = store.proxy_from_key(key)

    lifetime = ContextLifetime(store)
    lifetime.close()

    with pytest.raises(RuntimeError):
        lifetime.add_key(key)

    with pytest.raises(RuntimeError):
        lifetime.add_proxy(proxy)


@pytest.mark.parametrize(
    'expiry',
    # All of these times are either "now" or in the past.
    (datetime.fromtimestamp(0), timedelta(seconds=0), 0.0, -1),
)
def test_lease_lifetime_closes_after_expiry(
    store: Store[LocalConnector],
    expiry: Any,
) -> None:
    lifetime = LeaseLifetime(store, expiry=expiry)
    time.sleep(0.001)
    assert lifetime.done()

    # Close is idempotent
    lifetime.close()


@pytest.mark.parametrize(
    'expiry',
    (
        datetime.fromtimestamp(time.time() - 0.001),
        timedelta(milliseconds=1),
        0.001,
    ),
)
def test_lease_lifetime_extend(
    store: Store[LocalConnector],
    expiry: Any,
) -> None:
    lifetime = LeaseLifetime(store, expiry=0.001)

    assert lifetime._timer is not None
    first_timer = lifetime._timer

    lifetime.extend(expiry)

    first_timer.join()
    time.sleep(0.001)

    try:
        # Wait on possible second timer
        lifetime._timer.join()
    except AttributeError:  # pragma: no cover
        # Raised if lifetime._timer is None because it has already been closed.
        pass

    assert lifetime.done()


@pytest.mark.parametrize('close_store', (True, False))
def test_register_lifetime_atexit(
    store: Store[LocalConnector],
    close_store: bool,
) -> None:
    key = store.put('value')

    lifetime = ContextLifetime(store)
    lifetime.add_key(key)

    callback = register_lifetime_atexit(lifetime, close_store=close_store)

    assert not lifetime.done()
    callback()
    assert lifetime.done()
    assert not store.exists(key)

    atexit.unregister(callback)

from __future__ import annotations

import atexit
import contextlib
import pathlib
import subprocess
import sys
import textwrap
import threading
import time
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest import mock

import pytest

from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.exceptions import ProxyStoreFactoryError
from proxystore.store.lifetimes import ContextLifetime
from proxystore.store.lifetimes import LeaseLifetime
from proxystore.store.lifetimes import Lifetime
from proxystore.store.lifetimes import register_lifetime_atexit
from proxystore.store.lifetimes import StaticLifetime


def test_context_lifetime_protocol(store: Store[LocalConnector]) -> None:
    lifetime = ContextLifetime(store)
    assert isinstance(lifetime, Lifetime)
    lifetime.close()


def test_context_lifetime_repr(store: Store[LocalConnector]) -> None:
    with ContextLifetime(store, name='test-lifetime') as lifetime:
        assert repr(lifetime) == (
            f'Lifetime(name=test-lifetime, store={store!r})'
        )


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

    with (
        ContextLifetime(store) as lifetime,
        pytest.raises(ProxyStoreFactoryError),
    ):
        lifetime.add_proxy(proxy)


def test_context_lifetime_add_key_during_close(
    store: Store[LocalConnector],
) -> None:
    lifetime = ContextLifetime(store)
    lifetime.add_key(store.put('value'))

    errors: list[Exception] = []

    def _add_key() -> None:
        try:
            lifetime.add_key(store.put('other'))
        except RuntimeError as e:
            errors.append(e)

    thread = threading.Thread(target=_add_key)

    def _evict(key: Any) -> None:
        # Add a key from another thread while close() is evicting. The
        # thread should wait for close() to finish rather than modifying
        # the set of keys being iterated over.
        thread.start()
        thread.join(timeout=0.1)
        assert thread.is_alive()

    with mock.patch.object(store, 'evict', side_effect=_evict):
        lifetime.close()

    thread.join()
    assert len(errors) == 1
    assert 'Lifetime has ended' in str(errors[0])


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
    # Use an initial expiry far enough in the future that the background
    # timer cannot fire and close the lifetime before extend() is called
    # below. A very short initial expiry (e.g. 0.001) races with the main
    # thread reaching extend() on slow/loaded runners, causing extend() to
    # hit the "lifetime has ended" guard (flaky on macOS). The relative
    # extend values still expire quickly after this base.
    initial_expiry = 0.1
    lifetime = LeaseLifetime(store, expiry=initial_expiry)

    assert lifetime._timer is not None
    first_timer = lifetime._timer

    lifetime.extend(expiry)

    first_timer.join()
    time.sleep(0.001)

    # Wait on possible second timer. AttributeError is raised if
    # lifetime._timer is None because it has already been closed.
    with contextlib.suppress(AttributeError):
        lifetime._timer.join()

    assert lifetime.done()


def test_lease_lifetime_does_not_block_exit(tmp_path: pathlib.Path) -> None:
    store_dir = tmp_path / 'store'
    code = textwrap.dedent(
        f"""\
        from proxystore.connectors.file import FileConnector
        from proxystore.store import Store
        from proxystore.store.lifetimes import LeaseLifetime

        store = Store(FileConnector({str(store_dir)!r}, clear=False))
        lifetime = LeaseLifetime(store, expiry=60)
        store.put('value', lifetime=lifetime)
        """,
    )
    start = time.perf_counter()
    subprocess.run([sys.executable, '-c', code], check=True, timeout=30)
    # The process should exit without waiting for the lease to expire.
    assert time.perf_counter() - start < 30
    # The lease should have been closed at exit, evicting the object.
    assert list(store_dir.iterdir()) == []


def test_lease_lifetime_close_unregisters_atexit(
    store: Store[LocalConnector],
) -> None:
    with mock.patch('proxystore.store.lifetimes.atexit') as mock_atexit:
        lifetime = LeaseLifetime(store, expiry=60)
        mock_atexit.register.assert_called_once_with(lifetime._callback)
        assert lifetime._timer is not None
        assert lifetime._timer.daemon
        lifetime.close()
        mock_atexit.unregister.assert_called_once_with(lifetime._callback)


@pytest.mark.parametrize('close_store', (True, False))
def test_register_lifetime_atexit(
    store: Store[LocalConnector],
    close_store: bool,
) -> None:
    key = store.put('value')

    lifetime = ContextLifetime(store)
    lifetime.add_key(key)

    callback = register_lifetime_atexit(lifetime, close_stores=close_store)

    assert not lifetime.done()
    callback()
    assert lifetime.done()
    assert not store.exists(key)

    atexit.unregister(callback)


def test_static_lifetime_is_singleton() -> None:
    assert StaticLifetime() is StaticLifetime()


def test_add_key_without_store_error() -> None:
    with pytest.raises(ValueError, match='requires the store parameter'):
        StaticLifetime().add_key(())


def test_static_lifetime_add_bad_proxy() -> None:
    proxy: Proxy[list[Any]] = Proxy(list)

    with pytest.raises(ProxyStoreFactoryError):
        StaticLifetime().add_proxy(proxy)


def test_static_lifetime_cleanup(store: Store[LocalConnector]) -> None:
    key1 = store.put('value1')
    key2 = store.put('value2')
    key3 = store.put('value3')
    key4 = store.put('value4')
    proxy1: Proxy[str] = store.proxy_from_key(key3)
    proxy2: Proxy[str] = store.proxy_from_key(key4)

    lifetime = StaticLifetime()
    assert not lifetime.done()

    lifetime.add_key(key1, key2, store=store)
    lifetime.add_proxy(proxy1, proxy2)

    # Will get back same lifetime object so both options to close work
    # and are idempotent.
    StaticLifetime().close()
    lifetime.close()
    assert lifetime.done()

    assert not store.exists(key1)
    assert not store.exists(key2)
    assert not store.exists(key3)
    assert not store.exists(key4)

    # Cleanup singleton instance
    StaticLifetime._instance = None


def test_static_lifetime_close_store(store: Store[LocalConnector]) -> None:
    store.put('value', lifetime=StaticLifetime())

    StaticLifetime().close(close_stores=True)

    # Cleanup singleton instance
    StaticLifetime._instance = None

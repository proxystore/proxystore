"""Proxy Unit Tests."""
from __future__ import annotations

import pickle as pkl

import numpy as np
import pytest

import proxystore as ps
from proxystore.factory import SimpleFactory
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import deserialize
from proxystore.serialize import serialize


def test_proxy() -> None:
    """Test Proxy behavior."""
    with pytest.raises(TypeError):
        # Proxy requires a callable type
        Proxy('not a factory')  # type: ignore

    x = np.array([1, 2, 3])
    f = SimpleFactory(x)
    p = Proxy(f)

    assert not ps.proxy.is_resolved(p)

    # Test pickleable
    p_pkl = pkl.dumps(p)
    p = pkl.loads(p_pkl)

    assert not ps.proxy.is_resolved(p)

    assert isinstance(p, Proxy)
    assert isinstance(p, np.ndarray)
    assert ps.proxy.is_resolved(p)

    # Test extracting
    x_ = ps.proxy.extract(p)
    assert isinstance(x_, np.ndarray)
    assert not isinstance(x_, Proxy)
    assert np.array_equal(x, x_)

    p = p + 1
    assert not isinstance(p, Proxy)
    assert np.array_equal(p, [2, 3, 4])
    assert len(p) == 3
    assert np.sum(p) == 9

    # Adding two proxies returns type of wrapped
    p = Proxy(f)
    p = p + p
    assert np.sum(p) == 12
    assert isinstance(p, np.ndarray)
    assert not isinstance(p, Proxy)

    def double(y):
        return 2 * y

    p = Proxy(f)
    res = double(p)
    assert not isinstance(res, Proxy)
    assert np.array_equal(res, [2, 4, 6])

    p = Proxy(SimpleFactory([np.array([1, 2, 3]), np.array([2, 3, 4])]))
    res = np.sum(p, axis=0)
    assert not isinstance(res, Proxy)
    assert np.array_equal(res, [3, 5, 7])

    p = Proxy(f)
    assert isinstance(p, np.ndarray)

    p = Proxy(SimpleFactory('hello'))
    assert not ps.proxy.is_resolved(p)
    ps.proxy.resolve(p)
    assert ps.proxy.is_resolved(p)


def test_proxy_locker():
    value = [1, 2, 3]
    proxy = Proxy(SimpleFactory(value))

    locker = ProxyLocker(proxy)
    res = locker.unlock()
    assert isinstance(res, Proxy)
    assert res == value


def test_proxy_locker_attr_access():
    value = [1, 2, 3]
    proxy = Proxy(SimpleFactory(value))
    locker = ProxyLocker(proxy)

    with pytest.raises(AttributeError):
        locker._proxy[0]

    assert not is_resolved(locker.unlock())

    # Not _proxy attributes should still work normally
    locker._test = 1  # type: ignore
    assert locker._test == 1


def test_proxy_locker_serialization():
    value = [1, 2, 3]
    proxy = Proxy(SimpleFactory(value))
    locker = ProxyLocker(proxy)
    assert not is_resolved(locker.unlock())

    locker = deserialize(serialize(locker))
    assert not is_resolved(locker.unlock())

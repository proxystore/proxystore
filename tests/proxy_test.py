from __future__ import annotations

import pickle as pkl

import pytest

import proxystore as ps
from proxystore.factory import SimpleFactory
from proxystore.proxy import is_resolved
from proxystore.proxy import Proxy
from proxystore.proxy import ProxyLocker
from proxystore.serialize import deserialize
from proxystore.serialize import serialize


def test_proxy() -> None:
    with pytest.raises(TypeError):
        # Proxy requires a callable type
        Proxy('not a factory')  # type: ignore

    x = [1, 2, 3]
    f = SimpleFactory(x)
    p = Proxy(f)

    assert not ps.proxy.is_resolved(p)

    # Test pickleable
    p_pkl = pkl.dumps(p)
    p = pkl.loads(p_pkl)

    assert not ps.proxy.is_resolved(p)

    assert isinstance(p, Proxy)
    assert isinstance(p, list)
    assert ps.proxy.is_resolved(p)

    # Test extracting
    x_ = ps.proxy.extract(p)
    assert isinstance(x_, list)
    assert not isinstance(x_, Proxy)
    assert x == x_

    p = p + [1]  # noqa
    assert not isinstance(p, Proxy)
    assert p == [1, 2, 3, 1]
    assert len(p) == 4
    assert sum(p) == 7

    # Adding two proxies returns type of wrapped
    p = Proxy(f)
    p = p + p
    assert sum(p) == 12
    assert isinstance(p, list)
    assert not isinstance(p, Proxy)

    def double(y):
        return [2 * x for x in y]

    p = Proxy(f)
    res = double(p)
    assert not isinstance(res, Proxy)
    assert res == [2, 4, 6]

    p = Proxy(f)
    assert isinstance(p, list)

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

"""Proxy Unit Tests."""
import pickle as pkl

import numpy as np
from pytest import raises

import proxystore as ps
from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy


def test_proxy() -> None:
    """Test Proxy behavior."""
    with raises(TypeError):
        # Proxy requires type BaseFactory
        Proxy(lambda: "fake object")

    x = np.array([1, 2, 3])
    f = SimpleFactory(x)
    p = Proxy(f)

    assert not ps.proxy.is_resolved(p)
    # BaseFactory does not use a key like KeyFactory or RedisFactory
    assert ps.proxy.get_key(p) is None

    # Test pickleable
    p_pkl = pkl.dumps(p)
    p = pkl.loads(p_pkl)

    assert not ps.proxy.is_resolved(p)

    # Test async
    ps.proxy.resolve_async(p)
    assert p[0] == 1
    # Now async resolve should be a no-op
    ps.proxy.resolve_async(p)
    assert p[1] == 2

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

    p = Proxy(SimpleFactory("hello"))
    assert not ps.proxy.is_resolved(p)
    ps.proxy.resolve(p)
    assert ps.proxy.is_resolved(p)

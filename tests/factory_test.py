from __future__ import annotations

import proxystore as ps
from proxystore.factory import LambdaFactory
from proxystore.factory import SimpleFactory


def test_simple_factory() -> None:
    f = SimpleFactory([1, 2, 3])

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_pkl)
    assert f() == [1, 2, 3]


def test_lambda_factory() -> None:
    f1 = LambdaFactory(lambda: [1, 2, 3])

    # Test callable
    assert f1() == [1, 2, 3]

    # Test pickleable
    f1_pkl = ps.serialize.serialize(f1)
    f1 = ps.serialize.deserialize(f1_pkl)
    assert f1() == [1, 2, 3]

    # Test with function
    def myfunc() -> str:
        return 'abc'

    f2 = LambdaFactory(myfunc)
    f2_pkl = ps.serialize.serialize(f2)
    f2 = ps.serialize.deserialize(f2_pkl)
    assert f2() == 'abc'

    # Test args/kwargs
    def power(a, b):
        return a**b

    f3 = LambdaFactory(power, 2, 3)
    assert f3() == 8

    f3 = LambdaFactory(power, a=2, b=4)
    assert f3() == 16

    f3 = LambdaFactory(power, 2, b=5)
    assert f3() == 32

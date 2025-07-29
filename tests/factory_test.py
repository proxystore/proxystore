from __future__ import annotations

from proxystore.factory import LambdaFactory
from proxystore.factory import SimpleFactory
from proxystore.serialize import deserialize
from proxystore.serialize import serialize


def test_simple_factory() -> None:
    f = SimpleFactory([1, 2, 3])

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = serialize(f)
    f = deserialize(f_pkl)
    assert f() == [1, 2, 3]


def test_lambda_factory() -> None:
    f1 = LambdaFactory(lambda: [1, 2, 3])

    # Test callable
    assert f1() == [1, 2, 3]

    # Test pickleable
    f1_pkl = serialize(f1)
    f1 = deserialize(f1_pkl)
    assert f1() == [1, 2, 3]

    # Test with function
    def myfunc() -> str:
        return 'abc'

    f2 = LambdaFactory(myfunc)
    f2_pkl = serialize(f2)
    f2 = deserialize(f2_pkl)
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

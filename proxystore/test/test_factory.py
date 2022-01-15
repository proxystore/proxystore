"""Factory Unit Tests."""
import proxystore as ps
from proxystore.factory import LambdaFactory
from proxystore.factory import SimpleFactory


def test_simple_factory() -> None:
    """Test SimpleFactory."""
    f = SimpleFactory([1, 2, 3])

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_pkl)
    assert f() == [1, 2, 3]

    # Test async resolve
    f.resolve_async()
    assert f() == [1, 2, 3]


def test_lambda_factory() -> None:
    """Test LambdaFactory."""
    f = LambdaFactory(lambda: [1, 2, 3])

    # Test callable
    assert f() == [1, 2, 3]

    # Test pickleable
    f_pkl = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_pkl)
    assert f() == [1, 2, 3]

    # Test async resolve
    f.resolve_async()
    assert f() == [1, 2, 3]

    # Test with function
    def myfunc() -> str:
        return "abc"

    f = LambdaFactory(myfunc)
    f_pkl = ps.serialize.serialize(f)
    f = ps.serialize.deserialize(f_pkl)
    assert f() == "abc"

    # Test args/kwargs
    def power(a, b):
        return a ** b

    f = LambdaFactory(power, 2, 3)
    assert f() == 8

    f = LambdaFactory(power, a=2, b=4)
    assert f() == 16

    f = LambdaFactory(power, 2, b=5)
    assert f() == 32

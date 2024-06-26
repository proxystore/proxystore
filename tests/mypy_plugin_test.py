from __future__ import annotations

import sys
from types import MappingProxyType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import assert_type
else:  # pragma: <3.11 cover
    from typing_extensions import assert_type

import pytest

from proxystore.proxy import Proxy
from proxystore.proxy import ProxyOr

T = TypeVar('T')


def test_proxy_slots_types() -> None:
    def factory_int() -> int:
        return 42

    proxy = Proxy(factory_int)

    assert_type(proxy, Proxy[int])
    assert_type(proxy.__proxy_factory__, Callable[[], int])
    assert_type(proxy.__proxy_resolved__, bool)
    assert_type(proxy.__proxy_wrapped__, int)

    if TYPE_CHECKING:
        # Accessing __proxy_target__ directly will raise an AttributeError
        # because at runtime because it must be done through
        # object.__getattribute__(proxy, '__proxy_target__').
        assert_type(proxy.__proxy_target__, int)


def test_proxy_class_attribute_types() -> None:
    # Note that it doesn't actually matter if these branches run because
    # assert_type is a no-op at runtime. We just need mypy to be able
    # to statically parse it.
    if sys.version_info >= (3, 10):  # pragma: no cover
        assert_type(Proxy.__annotations__, dict[str, Any])
        assert_type(Proxy.__dict__, MappingProxyType[str, Any])
        assert_type(Proxy.__doc__, str | None)

    assert_type(Proxy.__module__, str)


def test_proxy_inherits_attributes_of_target() -> None:
    class TestClass:
        def __init__(self, attr_str: str) -> None:
            self.attr_str = attr_str

        def method_none(self) -> None:
            pass

        def method_int(self) -> int:
            return 42

    def factory() -> TestClass:
        return TestClass('attr')

    proxy = Proxy(factory)

    assert_type(proxy, Proxy[TestClass])
    assert_type(proxy.attr_str, str)
    assert_type(proxy.method_none(), None)
    assert_type(proxy.method_int(), int)


def test_proxy_inherits_attributes_of_generic_target() -> None:
    class TestClass(Generic[T]):
        def __init__(self, attr: T) -> None:
            self.attr = attr

        def method(self) -> T:
            return self.attr

    def factory() -> TestClass[str]:
        return TestClass('attr')

    proxy = Proxy(factory)

    assert_type(proxy, Proxy[TestClass[str]])
    assert_type(proxy.attr, str)
    assert_type(proxy.method(), str)


def test_proxy_target_generic_method() -> None:
    class TestClass:
        def method(self, x: T) -> T:
            return x

    def factory() -> TestClass:
        return TestClass()

    proxy = Proxy(factory)

    assert_type(proxy, Proxy[TestClass])
    assert_type(proxy.method(42), int)
    assert_type(proxy.method('str'), str)


def test_proxy_bad_attribute_use() -> None:
    # This tests abuses the fact that we run mypy with --warn-unused-ignores
    # so anywhere we have a # type: ignore[variant] comment that line
    # was guaranteed to have raised a mypy error.
    # See the guide in typing for more details:
    # https://typing.readthedocs.io/en/latest/source/quality.html#testing-using-assert-type-and-warn-unused-ignores
    class TestClass:
        def __init__(self, attr_str: str) -> None:
            self.attr_str = attr_str

        def method_int(self) -> int:
            return 42

    def factory() -> TestClass:
        return TestClass('attr')

    proxy = Proxy(factory)

    assert_type(proxy, Proxy[TestClass])
    assert_type(proxy.attr_str, str)
    # TestClass.attr_str is not callable so we mark this as a type error
    # to be ignored.
    with pytest.raises(TypeError, match="'str' object is not callable"):
        proxy.attr_str()  # type: ignore[operator]

    assert_type(proxy.method_int(), int)
    x = proxy.method_int()

    def expects_str(value: str) -> None:
        pass

    # Will fail because mypy should have inferred x is an int.
    expects_str(x)  # type: ignore[arg-type]


def test_union_type_bad_attribute_crash() -> None:
    # https://github.com/proxystore/proxystore/issues/559
    def foo(x: T | Proxy[T]) -> T | Callable[[], T]:  # pragma: no cover
        # Note: this function should not actually be called! It will error!
        if isinstance(x, Proxy):
            return x.__factory__  # In Issue #559, mypy crashes on this line
        else:
            return x

    x = Proxy(lambda: 42)
    try:
        _ = x.__factory__  # type: ignore[attr-defined]
    except AttributeError:
        pass


def test_union_type_partial_attr() -> None:
    class Foo:
        x = 0

    class Bar:
        y = ''

    union: Proxy[Foo] | Bar = Proxy(lambda: Foo())

    try:
        # Item "Proxy[Foo]" of "Proxy[Foo] | Bar" has no attribute "y"
        assert isinstance(union.y, str)  # type: ignore[union-attr]
    except AttributeError:
        pass

    # Item "Bar" of "Proxy[Foo] | Bar" has no attribute "x"  [union-attr]
    assert isinstance(union.x, int)  # type: ignore[union-attr]


def test_proxy_or_type() -> None:
    value: ProxyOr[str] = 'hello'
    assert value.startswith('hello')

    value = Proxy(lambda: 'hello')
    assert value.startswith('hello')

    class TestClass:
        value = 42

    def get_value(x: ProxyOr[TestClass]) -> int:
        assert_type(x.value, int)
        return x.value

    class_instance = TestClass()
    proxy_instance = Proxy(lambda: TestClass())

    assert get_value(class_instance) == 42
    assert get_value(proxy_instance) == 42

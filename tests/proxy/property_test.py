from __future__ import annotations

import pytest

from proxystore.proxy._property import proxy_property


def test_decorator_implicit_call() -> None:
    class _Foo:
        @proxy_property
        def _bar(self) -> str:
            return 'instance'

    with pytest.raises(AttributeError, match='no default value'):
        assert _Foo._bar != 'instance'

    assert _Foo()._bar == 'instance'


def test_decorator_explicit_call() -> None:
    class _Foo:
        # MyPy doesn't like this form and raises:
        #   tests/proxy/property_test.py:22: error:
        #   Argument 1 has incompatible type "Callable[[_Foo], str]";
        #   expected "Callable[[_Foo], Never]"  [arg-type]
        # I believe this is because the TypeVar T of the function and default
        # params of proxy_property() cannot be bound to when both params
        # default to None in this case.
        @proxy_property()  # type: ignore[arg-type]
        def _bar(self) -> str:
            return 'instance'

    with pytest.raises(AttributeError, match='no default value'):
        assert _Foo._bar != 'instance'

    assert _Foo()._bar == 'instance'


def test_decorate_with_default() -> None:
    class _Foo:
        @proxy_property(default='class')
        def _bar(self) -> str:
            return 'instance'

    assert _Foo._bar == 'class'
    assert _Foo()._bar == 'instance'


def test_get_set_del() -> None:
    class _Foo:
        def __init__(self, bar_value: str) -> None:
            self._bar_value = bar_value

        @proxy_property(default='class')
        def _bar(self) -> str:
            return self._bar_value

        @_bar.setter
        def _set_bar(self, value: str) -> None:
            self._bar_value = value

        @_bar.deleter
        def _del_bar(self) -> None:
            del self._bar_value

    foo = _Foo('instance')
    assert foo._bar == 'instance'
    foo._bar = 'new-instance'
    assert foo._bar == 'new-instance'
    del foo._bar
    with pytest.raises(AttributeError, match='_bar'):
        assert foo._bar == 'new-instance'


def test_set_attribute_error() -> None:
    class _Foo:
        @proxy_property(default='class')
        def _bar(self) -> str:
            raise AssertionError

    match = "property '_bar' of '_Foo' object has no setter"
    with pytest.raises(AttributeError, match=match):
        _Foo()._bar = 'new'


def test_del_attribute_error() -> None:
    class _Foo:
        @proxy_property(default='class')
        def _bar(self) -> str:
            raise AssertionError

    match = "property '_bar' of '_Foo' object has no deleter"
    with pytest.raises(AttributeError, match=match):
        del _Foo()._bar

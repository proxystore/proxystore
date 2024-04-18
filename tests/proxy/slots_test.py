# This module contains source code from python-lazy-object-proxy v1.10.0
# which is available under the BSD 2-Clause License included below.
#
# The following modifications to the source has been made:
#   * Replaces use of the Proxy type from lazy-object-proxy with the
#     native implementation in ProxyStore.
#   * Consolidated source from multiple modules into this single module.
#   * Consolidated, updated, and/or removed tests.
#   * Code formatting and additional type annotations.
#
# Source: https://github.com/ionelmc/python-lazy-object-proxy/tree/v1.10.0
#
# BSD 2-Clause License
#
# Copyright (c) 2014-2023, Ionel Cristian Mărieș. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# mypy: ignore-errors
from __future__ import annotations

import decimal
import fractions
import gc
import os
import pickle
import sys
import types
import weakref
from datetime import date
from datetime import datetime

import pytest

from proxystore.proxy._slots import SlotsProxy

OBJECTS_CODE = """
class TargetBaseClass(object):
    '''Docstring'''
    pass

class Target(TargetBaseClass):
    '''Docstring'''
    pass

def target():
    '''Docstring'''
    pass
"""

objects = types.ModuleType('objects')
exec(OBJECTS_CODE, objects.__dict__, objects.__dict__)


def test_round() -> None:
    proxy = SlotsProxy(lambda: 1.2)
    assert round(proxy) == 1


def test_get_wrapped() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = SlotsProxy(lambda: function1)

    assert function2.__wrapped__ == function1

    function3 = SlotsProxy(lambda: function2)

    assert function3.__wrapped__ == function1


def test_set_wrapped() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = SlotsProxy(lambda: function1)

    assert function2 == function1
    assert function2.__wrapped__ is function1
    assert function2.__name__ == function1.__name__

    assert function2.__qualname__ == function1.__qualname__

    function2.__wrapped__ = None

    assert not hasattr(function1, '__wrapped__')

    assert function2 == None  # noqa
    assert function2.__wrapped__ is None
    assert not hasattr(function2, '__name__')

    assert not hasattr(function2, '__qualname__')

    def function3(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2.__wrapped__ = function3

    assert function2 == function3
    assert function2.__wrapped__ == function3
    assert function2.__name__ == function3.__name__

    assert function2.__qualname__ == function3.__qualname__


def test_wrapped_attribute() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = SlotsProxy(lambda: function1)

    function2.variable = True

    assert hasattr(function1, 'variable')
    assert hasattr(function2, 'variable')

    assert function2.variable is True

    del function2.variable

    assert not hasattr(function1, 'variable')
    assert not hasattr(function2, 'variable')

    assert getattr(function2, 'variable', None) is None


@pytest.mark.parametrize('kind', ('class', 'instance', 'function'))
def test_special_writeable_attributes(kind: str) -> None:
    # https://docs.python.org/3/reference/datamodel.html#special-writable-attributes
    class TestClass:
        """Test class."""

        pass

    def test_function() -> None:  # pragma: no cover
        """Test function."""
        pass

    if kind == 'class':
        target = TestClass
    elif kind == 'instance':
        target = TestClass()
    elif kind == 'function':
        target = test_function
    else:
        raise AssertionError()

    wrapper = SlotsProxy(lambda: target)

    if kind != 'instance':
        assert wrapper.__name__ == target.__name__
        assert wrapper.__qualname__ == target.__qualname__
        assert wrapper.__annotations__ == target.__annotations__

    if kind != 'function':
        assert wrapper.__weakref__ == target.__weakref__

    assert wrapper.__module__ == target.__module__
    assert wrapper.__doc__ == target.__doc__

    if kind != 'instance':
        new_name = 'new-name'
        wrapper.__name__ = new_name
        assert wrapper.__name__ == target.__name__ == new_name

        new_ann = {}
        wrapper.__annotations__ = new_ann
        assert wrapper.__annotations__ == target.__annotations__ == new_ann

    new_module = 'new-module'
    wrapper.__module__ = new_module
    assert wrapper.__module__ == target.__module__ == new_module

    new_doc = 'new-doc'
    wrapper.__doc__ = new_doc
    assert wrapper.__doc__ == target.__doc__ == new_doc


def test_class_of_class() -> None:
    # Test preservation of class __class__ attribute.

    target = objects.Target
    wrapper = SlotsProxy(lambda: target)

    assert wrapper.__class__ is target.__class__

    assert isinstance(wrapper, type(target))


def test_revert_class_proxying() -> None:
    class ProxyWithOldStyleIsInstance(SlotsProxy):
        __class__ = object.__dict__['__class__']

    target = objects.Target()
    wrapper = ProxyWithOldStyleIsInstance(lambda: target)  # pragma: no cover

    assert wrapper.__class__ is ProxyWithOldStyleIsInstance

    assert isinstance(wrapper, ProxyWithOldStyleIsInstance)
    assert not isinstance(wrapper, objects.Target)
    assert not isinstance(wrapper, objects.TargetBaseClass)

    class ProxyWithOldStyleIsInstance2(ProxyWithOldStyleIsInstance):
        pass

    wrapper = ProxyWithOldStyleIsInstance2(lambda: target)  # pragma: no cover

    assert wrapper.__class__ is ProxyWithOldStyleIsInstance2

    assert isinstance(wrapper, ProxyWithOldStyleIsInstance2)
    assert not isinstance(wrapper, objects.Target)
    assert not isinstance(wrapper, objects.TargetBaseClass)


def test_class_of_instance() -> None:
    # Test preservation of instance __class__ attribute.

    target = objects.Target()
    wrapper = SlotsProxy(lambda: target)

    assert wrapper.__class__ is target.__class__

    assert isinstance(wrapper, objects.Target)
    assert isinstance(wrapper, objects.TargetBaseClass)


def test_class_of_function() -> None:
    # Test preservation of function __class__ attribute.

    target = objects.target
    wrapper = SlotsProxy(lambda: target)

    assert wrapper.__class__ is target.__class__

    assert isinstance(wrapper, type(target))


def test_dir_of_class() -> None:
    # Test preservation of class __dir__ attribute.

    target = objects.Target
    wrapper = SlotsProxy(lambda: target)

    assert dir(wrapper) == dir(target)


def test_vars_of_class() -> None:
    # Test preservation of class __dir__ attribute.

    target = objects.Target
    wrapper = SlotsProxy(lambda: target)

    assert vars(wrapper) == vars(target)


def test_dir_of_instance() -> None:
    # Test preservation of instance __dir__ attribute.

    target = objects.Target()
    wrapper = SlotsProxy(lambda: target)

    assert dir(wrapper) == dir(target)


def test_vars_of_instance() -> None:
    # Test preservation of instance __dir__ attribute.

    target = objects.Target()
    wrapper = SlotsProxy(lambda: target)

    assert vars(wrapper) == vars(target)


def test_dir_of_function() -> None:
    # Test preservation of function __dir__ attribute.

    target = objects.target
    wrapper = SlotsProxy(lambda: target)

    assert dir(wrapper) == dir(target)


def test_vars_of_function() -> None:
    # Test preservation of function __dir__ attribute.

    target = objects.target
    wrapper = SlotsProxy(lambda: target)

    assert vars(wrapper) == vars(target)


def test_function_no_args() -> None:
    _args = ()
    _kwargs = {}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = SlotsProxy(lambda: function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_function_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = SlotsProxy(lambda: function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_function_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = SlotsProxy(lambda: function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_function_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = SlotsProxy(lambda: function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_no_args() -> None:
    _args = ()
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_instancemethod_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_instancemethod_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_no_args() -> None:
    _args = ()
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(Class())

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(Class(), *_args)

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(Class(), **_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(Class(), *_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_no_args() -> None:
    _args = ()
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_classmethod_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_classmethod_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_via_class_no_args() -> None:
    _args = ()
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_classmethod_via_class_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_classmethod_via_class_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_via_class_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_no_args() -> None:
    _args = ()
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_staticmethod_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_staticmethod_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class().function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_no_args() -> None:
    _args = ()
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_args() -> None:
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_kwargs() -> None:
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_args_plus_kwargs() -> None:
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = SlotsProxy(lambda: Class.function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_iteration() -> None:
    items = [1, 2]

    wrapper = SlotsProxy(lambda: items)

    result = [x for x in wrapper]  # noqa: C416

    assert result == items

    with pytest.raises(TypeError):
        iter(SlotsProxy(lambda: 1))


def test_next() -> None:
    class TestClass:
        value = 1

        def __next__(self) -> int:
            return self.value

    wrapper = SlotsProxy(lambda: TestClass())
    assert next(wrapper) == 1


def test_iter_builtin() -> None:
    iter(SlotsProxy(lambda: [1, 2]))
    pytest.raises(TypeError, iter, SlotsProxy(lambda: 1))


def test_context_manager() -> None:
    class Class:
        def __enter__(self):
            return self

        def __exit__(*args, **kwargs):
            return

    instance = Class()

    wrapper = SlotsProxy(lambda: instance)

    with wrapper:
        pass


def test_object_hash() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = SlotsProxy(lambda: function1)

    assert hash(function2) == hash(function1)


def test_mapping_key() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = SlotsProxy(lambda: function1)

    table = {function1: True}

    assert table.get(function2)

    table = {function2: True}

    assert table.get(function1)


def test_comparison() -> None:
    one = SlotsProxy(lambda: 1)
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert two > 1
    assert two >= 1
    assert two < 3
    assert two <= 3
    assert two != 1
    assert two == 2
    assert two != 3

    assert one < 2
    assert one <= 2
    assert three > 2
    assert three >= 2
    assert one != 2
    assert two == 2
    assert three != 2

    assert two > one
    assert two >= one
    assert two < three
    assert two <= three
    assert two != one
    assert two == two
    assert two != three


def test_int() -> None:
    one = SlotsProxy(lambda: 1)

    assert int(one) == 1


def test_float() -> None:
    one = SlotsProxy(lambda: 1)

    assert float(one) == 1.0


def test_add() -> None:
    one = SlotsProxy(lambda: 1)
    two = SlotsProxy(lambda: 2)

    assert one + two == 1 + 2
    assert 1 + two == 1 + 2
    assert one + 2 == 1 + 2


def test_sub() -> None:
    one = SlotsProxy(lambda: 1)
    two = SlotsProxy(lambda: 2)

    assert one - two == 1 - 2
    assert 1 - two == 1 - 2
    assert one - 2 == 1 - 2


def test_mul() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert two * three == 2 * 3
    assert 2 * three == 2 * 3
    assert two * 3 == 2 * 3


def test_matmul() -> None:
    class MatmulClass:
        def __init__(self, value):
            self.value = value

        def __matmul__(self, other):
            return self.value * other.value

        def __rmatmul__(self, other):
            return other + self.value

    one = MatmulClass(123)
    two = MatmulClass(234)
    assert one @ two == 28782

    one = SlotsProxy(lambda: MatmulClass(123))
    two = SlotsProxy(lambda: MatmulClass(234))
    assert one @ two == 28782

    one = SlotsProxy(lambda: MatmulClass(123))
    two = MatmulClass(234)
    assert one @ two == 28782

    one = 123
    two = SlotsProxy(lambda: MatmulClass(234))
    assert one @ two == 357

    one = SlotsProxy(lambda: 123)
    two = SlotsProxy(lambda: MatmulClass(234))
    assert one @ two == 357


def test_div() -> None:
    # On Python 2 this will pick up div and on Python
    # 3 it will pick up truediv.

    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert two / three == 2 / 3
    assert 2 / three == 2 / 3
    assert two / 3 == 2 / 3


def test_divdiv() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three // two == 3 // 2
    assert 3 // two == 3 // 2
    assert three // 2 == 3 // 2


def test_mod() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three % two == 3 % 2
    assert 3 % two == 3 % 2
    assert three % 2 == 3 % 2


def test_divmod() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert divmod(three, two), divmod(3 == 2)
    assert divmod(3, two), divmod(3 == 2)
    assert divmod(three, 2), divmod(3 == 2)


def test_pow() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three**two == pow(3, 2)
    assert 3**two == pow(3, 2)
    assert pow(3, two) == pow(3, 2)
    assert three**2 == pow(3, 2)

    assert pow(three, two) == pow(3, 2)
    assert pow(3, two) == pow(3, 2)
    assert pow(three, 2) == pow(3, 2)
    assert pow(three, 2, 2) == pow(3, 2, 2)


def test_lshift() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three << two == 3 << 2
    assert 3 << two == 3 << 2
    assert three << 2 == 3 << 2


def test_rshift() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three >> two == 3 >> 2
    assert 3 >> two == 3 >> 2
    assert three >> 2 == 3 >> 2


def test_and() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three & two == 3 & 2
    assert 3 & two == 3 & 2
    assert three & 2 == 3 & 2


def test_xor() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three ^ two == 3 ^ 2
    assert 3 ^ two == 3 ^ 2
    assert three ^ 2 == 3 ^ 2


def test_or() -> None:
    two = SlotsProxy(lambda: 2)
    three = SlotsProxy(lambda: 3)

    assert three | two == 3 | 2
    assert 3 | two == 3 | 2
    assert three | 2 == 3 | 2


def test_iadd() -> None:
    value = SlotsProxy(lambda: 1)
    one = SlotsProxy(lambda: 1)

    value += 1
    assert value == 2
    assert type(value) == SlotsProxy

    value += one
    assert value == 3
    assert type(value) == SlotsProxy


def test_isub() -> None:
    value = SlotsProxy(lambda: 1)
    one = SlotsProxy(lambda: 1)

    value -= 1
    assert value == 0
    assert type(value) == SlotsProxy

    value -= one
    assert value == -1
    assert type(value) == SlotsProxy


def test_imul() -> None:
    value = SlotsProxy(lambda: 2)
    two = SlotsProxy(lambda: 2)

    value *= 2
    assert value == 4
    assert type(value) == SlotsProxy

    value *= two
    assert value == 8
    assert type(value) == SlotsProxy


def test_imatmul() -> None:
    class InplaceMatmul:
        value = None

        def __imatmul__(self, other):
            self.value = other
            return self

    value = InplaceMatmul()
    assert value.value is None
    value @= 123
    assert value.value == 123

    value = SlotsProxy(InplaceMatmul)
    value @= 234
    assert value.value == 234
    assert type(value) == SlotsProxy


def test_idiv() -> None:
    # On Python 2 this will pick up div and on Python
    # 3 it will pick up truediv.

    value = SlotsProxy(lambda: 2)
    two = SlotsProxy(lambda: 2)

    value /= 2
    assert value == 2 / 2
    assert type(value) == SlotsProxy

    value /= two
    assert value == 2 / 2 / 2
    assert type(value) == SlotsProxy


def test_ifloordiv() -> None:
    value = SlotsProxy(lambda: 2)
    two = SlotsProxy(lambda: 2)

    value //= 2
    assert value == 2 // 2
    assert type(value) == SlotsProxy

    value //= two
    assert value == 2 // 2 // 2
    assert type(value) == SlotsProxy


def test_imod() -> None:
    value = SlotsProxy(lambda: 10)
    two = SlotsProxy(lambda: 2)

    value %= 2
    assert value == 10 % 2
    assert type(value) == SlotsProxy

    value %= two
    assert value == 10 % 2 % 2
    assert type(value) == SlotsProxy


def test_ipow() -> None:
    value = SlotsProxy(lambda: 10)
    two = SlotsProxy(lambda: 2)

    value **= 2
    assert value == 10**2
    assert type(value) == SlotsProxy

    value **= two
    assert value == 10**2**2
    assert type(value) == SlotsProxy


def test_ilshift() -> None:
    value = SlotsProxy(lambda: 256)
    two = SlotsProxy(lambda: 2)

    value <<= 2
    assert value == 256 << 2
    assert type(value) == SlotsProxy

    value <<= two
    assert value == 256 << 2 << 2
    assert type(value) == SlotsProxy


def test_irshift() -> None:
    value = SlotsProxy(lambda: 2)
    two = SlotsProxy(lambda: 2)

    value >>= 2
    assert value == 2 >> 2
    assert type(value) == SlotsProxy

    value >>= two
    assert value == 2 >> 2 >> 2
    assert type(value) == SlotsProxy


def test_iand() -> None:
    value = SlotsProxy(lambda: 1)
    two = SlotsProxy(lambda: 2)

    value &= 2
    assert value == 1 & 2
    assert type(value) == SlotsProxy

    value &= two
    assert value == 1 & 2 & 2
    assert type(value) == SlotsProxy


def test_ixor() -> None:
    value = SlotsProxy(lambda: 1)
    two = SlotsProxy(lambda: 2)

    value ^= 2
    assert value == 1 ^ 2
    assert type(value) == SlotsProxy

    value ^= two
    assert value == 1 ^ 2 ^ 2
    assert type(value) == SlotsProxy


def test_ior() -> None:
    value = SlotsProxy(lambda: 1)
    two = SlotsProxy(lambda: 2)

    value |= 2
    assert value == 1 | 2
    assert type(value) == SlotsProxy

    value |= two
    assert value == 1 | 2 | 2
    assert type(value) == SlotsProxy


def test_neg() -> None:
    value = SlotsProxy(lambda: 1)

    assert -value == -1


def test_pos() -> None:
    value = SlotsProxy(lambda: 1)

    assert +value == 1


def test_abs() -> None:
    value = SlotsProxy(lambda: -1)

    assert abs(value) == 1


def test_invert() -> None:
    value = SlotsProxy(lambda: 1)

    assert ~value == ~1


def test_bool() -> None:
    value = SlotsProxy(lambda: True)

    assert bool(value)


def test_index() -> None:
    class TestClass1:
        def __index__(self):
            return 1

    value = SlotsProxy(lambda: TestClass1())
    items = [0, 1, 2]

    assert items[value] == items[1]

    class TestClass2:
        def __int__(self):
            return 1

    value = SlotsProxy(lambda: TestClass2())
    items = [0, 1, 2]

    assert items[value] == items[1]


def test_length() -> None:
    value = SlotsProxy(lambda: list(range(3)))

    assert len(value) == 3


def test_contains() -> None:
    value = SlotsProxy(lambda: list(range(3)))

    assert 2 in value
    assert -2 not in value


def test_getitem() -> None:
    value = SlotsProxy(lambda: list(range(3)))

    assert value[1] == 1


def test_setitem() -> None:
    value = SlotsProxy(lambda: list(range(3)))
    value[1] = -1

    assert value[1] == -1


def test_delitem() -> None:
    value = SlotsProxy(lambda: list(range(3)))

    assert len(value) == 3

    del value[1]

    assert len(value) == 2
    assert value[1] == 2


def test_getslice() -> None:
    value = SlotsProxy(lambda: list(range(5)))

    assert value[1:4] == [1, 2, 3]


def test_setslice() -> None:
    value = SlotsProxy(lambda: list(range(5)))

    value[1:4] = reversed(value[1:4])

    assert value[1:4] == [3, 2, 1]


def test_delslice() -> None:
    value = SlotsProxy(lambda: list(range(5)))

    del value[1:4]

    assert len(value) == 2
    assert value == [0, 4]


def test_dict_length() -> None:
    value = SlotsProxy(lambda: dict.fromkeys(range(3), False))

    assert len(value) == 3


def test_dict_contains() -> None:
    value = SlotsProxy(lambda: dict.fromkeys(range(3), False))

    assert 2 in value
    assert -2 not in value


def test_dict_getitem() -> None:
    value = SlotsProxy(lambda: dict.fromkeys(range(3), False))

    assert value[1] is False


def test_dict_setitem() -> None:
    value = SlotsProxy(lambda: dict.fromkeys(range(3), False))
    value[1] = True

    assert value[1] is True


def test_dict_delitem() -> None:
    value = SlotsProxy(lambda: dict.fromkeys(range(3), False))

    assert len(value) == 3

    del value[1]

    assert len(value) == 2


def test_str() -> None:
    value = SlotsProxy(lambda: 10)

    assert str(value) == str(10)

    value = SlotsProxy(lambda: (10,))

    assert str(value) == str((10,))

    value = SlotsProxy(lambda: [10])

    assert str(value) == str([10])

    value = SlotsProxy(lambda: {10: 10})

    assert str(value) == str({10: 10})


def test_repr() -> None:
    class Foobar:
        pass

    value = SlotsProxy(lambda: Foobar())
    str(value)
    representation = repr(value)
    print(representation)
    assert 'Proxy at' in representation
    assert 'lambda' in representation
    assert 'Foobar' in representation


def test_repr_doesnt_consume() -> None:
    consumed = []
    value = SlotsProxy(lambda: consumed.append(1))  # pragma: no cover
    print(repr(value))
    assert not consumed


def test_derived_new() -> None:
    class DerivedObjectProxy(SlotsProxy):
        def __new__(cls, wrapped):
            instance = super().__new__(cls)
            instance.__init__(wrapped)
            return instance

        def __init__(self, wrapped):
            super().__init__(wrapped)

    def function():
        return 123

    obj = DerivedObjectProxy(lambda: function)
    assert obj() == 123


def test_setup_class_attributes() -> None:
    def function():  # pragma: no cover
        pass

    class DerivedObjectProxy(SlotsProxy):
        pass

    obj = DerivedObjectProxy(lambda: function)

    DerivedObjectProxy.ATTRIBUTE = 1

    assert obj.ATTRIBUTE == 1
    assert not hasattr(function, 'ATTRIBUTE')

    del DerivedObjectProxy.ATTRIBUTE

    assert not hasattr(DerivedObjectProxy, 'ATTRIBUTE')
    assert not hasattr(obj, 'ATTRIBUTE')
    assert not hasattr(function, 'ATTRIBUTE')


def test_override_class_attributes() -> None:
    def function():  # pragma: no cover
        pass

    class DerivedObjectProxy(SlotsProxy):
        ATTRIBUTE = 1

    obj = DerivedObjectProxy(lambda: function)  # pragma: no cover

    assert DerivedObjectProxy.ATTRIBUTE == 1
    assert obj.ATTRIBUTE == 1

    obj.ATTRIBUTE = 2

    assert DerivedObjectProxy.ATTRIBUTE == 1

    assert obj.ATTRIBUTE == 2
    assert not hasattr(function, 'ATTRIBUTE')

    del DerivedObjectProxy.ATTRIBUTE

    assert not hasattr(DerivedObjectProxy, 'ATTRIBUTE')
    assert obj.ATTRIBUTE == 2
    assert not hasattr(function, 'ATTRIBUTE')


def test_attr_functions() -> None:
    def function():  # pragma: no cover
        pass

    proxy = SlotsProxy(lambda: function)  # pragma: no cover

    assert hasattr(proxy, '__getattr__')
    assert hasattr(proxy, '__setattr__')
    assert hasattr(proxy, '__delattr__')


def test_override_getattr() -> None:
    def function():  # pragma: no cover
        pass

    accessed = []

    class DerivedObjectProxy(SlotsProxy):
        def __getattr__(self, name):
            accessed.append(name)
            try:
                __getattr__ = super().__getattr__
            except AttributeError as e:  # pragma: no cover
                raise RuntimeError(str(e)) from e
            return __getattr__(name)

    function.attribute = 1

    proxy = DerivedObjectProxy(lambda: function)

    assert proxy.attribute == 1

    assert 'attribute' in accessed


def test_callable_proxy_hasattr_call() -> None:
    # TODO: this one is tricky...
    proxy = SlotsProxy(lambda: None)  # pragma: no cover

    assert callable(proxy)


def test_class_bytes() -> None:
    class Class:
        def __bytes__(self):
            return b'BYTES'

    instance = Class()

    proxy = SlotsProxy(lambda: instance)

    assert bytes(instance) == bytes(proxy)


def test_str_format() -> None:
    instance = 'abcd'

    proxy = SlotsProxy(lambda: instance)  # pragma: no cover

    assert format(instance, ''), format(proxy == '')


def test_list_reversed() -> None:
    instance = [1, 2]

    proxy = SlotsProxy(lambda: instance)

    assert list(reversed(instance)) == list(reversed(proxy))


def test_decimal_complex() -> None:
    instance = decimal.Decimal(123)

    proxy = SlotsProxy(lambda: instance)

    assert complex(instance) == complex(proxy)


def test_fractions_round() -> None:
    instance = fractions.Fraction('1/2')

    proxy = SlotsProxy(lambda: instance)

    assert round(instance) == round(proxy)


def test_readonly() -> None:
    proxy = SlotsProxy(lambda: object)
    assert proxy.__qualname__ == 'object'


def test_del_wrapped() -> None:
    foo = object()
    called = []

    def make_foo():
        called.append(1)
        return foo

    proxy = SlotsProxy(make_foo)
    str(proxy)
    assert called == [1]
    assert proxy.__wrapped__ is foo
    del proxy.__wrapped__
    str(proxy)
    assert called == [1, 1]


def test_raise_attribute_error() -> None:
    def foo():
        raise AttributeError('boom!')

    proxy = SlotsProxy(foo)
    pytest.raises(AttributeError, str, proxy)
    pytest.raises(AttributeError, lambda: proxy.__wrapped__)
    assert proxy.__factory__ is foo


def test_patching_the_factory() -> None:
    def foo():
        raise AttributeError('boom!')

    proxy = SlotsProxy(foo)
    pytest.raises(AttributeError, lambda: proxy.__wrapped__)
    assert proxy.__factory__ is foo

    proxy.__factory__ = lambda: foo
    pytest.raises(AttributeError, proxy)
    assert proxy.__wrapped__ is foo


def test_deleting_the_factory() -> None:
    proxy = SlotsProxy(None)
    assert proxy.__factory__ is None
    proxy.__factory__ = None
    assert proxy.__factory__ is None

    pytest.raises(TypeError, str, proxy)
    del proxy.__factory__
    pytest.raises(ValueError, str, proxy)


def test_patching_the_factory_with_none() -> None:
    proxy = SlotsProxy(None)
    assert proxy.__factory__ is None
    proxy.__factory__ = None
    assert proxy.__factory__ is None
    proxy.__factory__ = None
    assert proxy.__factory__ is None

    def foo():
        return 1

    proxy.__factory__ = foo
    assert proxy.__factory__ is foo
    assert proxy.__wrapped__ == 1
    assert str(proxy) == '1'


def test_new() -> None:
    a = SlotsProxy.__new__(SlotsProxy)
    b = SlotsProxy.__new__(SlotsProxy)
    # NOW KISS
    pytest.raises(ValueError, lambda: a + b)
    # no segfault, yay
    pytest.raises(ValueError, lambda: a.__wrapped__)


def test_set_wrapped_via_new() -> None:
    obj = SlotsProxy.__new__(SlotsProxy)
    obj.__wrapped__ = 1
    assert str(obj) == '1'
    assert obj + 1 == 2


def test_set_wrapped_regular() -> None:
    obj = SlotsProxy(None)
    obj.__wrapped__ = 1
    assert str(obj) == '1'
    assert obj + 1 == 2


@pytest.fixture(
    params=[
        'pickle',
    ],
)
def pickler(request):
    return pytest.importorskip(request.param)


@pytest.mark.parametrize(
    'obj',
    (
        1,
        1.2,
        'a',
        ['b', 'c'],
        {'d': 'e'},
        date(2015, 5, 1),
        datetime(2015, 5, 1),
        decimal.Decimal('1.2'),
    ),
)
@pytest.mark.parametrize('level', range(pickle.HIGHEST_PROTOCOL + 1))
def test_pickling(obj, pickler, level):
    proxy = SlotsProxy(lambda: obj)
    dump = pickler.dumps(proxy, protocol=level)
    result = pickler.loads(dump)
    assert obj == result


@pytest.mark.parametrize('level', range(pickle.HIGHEST_PROTOCOL + 1))
def test_pickling_exception(pickler, level):
    class TestError(Exception):
        pass

    def trouble_maker():
        raise TestError('foo')

    proxy = SlotsProxy(trouble_maker)
    with pytest.raises(TestError):
        pickler.dumps(proxy, protocol=level)


def test_garbage_collection() -> None:
    leaky = lambda: 'foobar'  # noqa
    proxy = SlotsProxy(leaky)
    leaky.leak = proxy
    ref = weakref.ref(leaky)
    assert proxy == 'foobar'
    del leaky
    del proxy
    gc.collect()
    assert ref() is None


def test_garbage_collection_count() -> None:
    obj = object()
    count = sys.getrefcount(obj)
    for _ in range(100):
        str(SlotsProxy(lambda: obj))
    assert count == sys.getrefcount(obj)


def test_subclassing_with_local_attr() -> None:
    class Foo:
        pass

    called = []

    class LazyProxy(SlotsProxy):
        name = None

        def __init__(self, func, **lazy_attr):
            super().__init__(func)
            for attr, val in lazy_attr.items():
                setattr(self, attr, val)

    proxy = LazyProxy(
        lambda: called.append(1) or Foo(),  # pragma: no cover
        name='bar',
    )
    assert proxy.name == 'bar'
    assert not called


def test_subclassing_dynamic_with_local_attr() -> None:
    class Foo:
        pass

    called = []

    class LazyProxy(SlotsProxy):
        def __init__(self, func, **lazy_attr):
            super().__init__(func)
            for attr, val in lazy_attr.items():
                object.__setattr__(self, attr, val)

    proxy = LazyProxy(
        lambda: called.append(1) or Foo(),  # pragma: no cover
        name='bar',
    )
    assert proxy.name == 'bar'
    assert not called


class FSPathMock:
    def __fspath__(self):
        return '/foobar'


def test_fspath() -> None:
    assert os.fspath(SlotsProxy(lambda: '/foobar')) == '/foobar'
    assert os.fspath(SlotsProxy(FSPathMock)) == '/foobar'
    with pytest.raises(TypeError) as excinfo:
        os.fspath(SlotsProxy(lambda: None))
    assert (
        '__fspath__() to return str or bytes, not NoneType'
        in excinfo.value.args[0]
    )


def test_fspath_method() -> None:
    assert SlotsProxy(FSPathMock).__fspath__() == '/foobar'


def test_resolved_new() -> None:
    obj = SlotsProxy.__new__(SlotsProxy)
    assert obj.__resolved__ is False


def test_resolved() -> None:
    obj = SlotsProxy(lambda: None)
    assert obj.__resolved__ is False
    assert obj.__wrapped__ is None
    assert obj.__resolved__ is True


def test_resolved_str() -> None:
    obj = SlotsProxy(lambda: None)
    assert obj.__resolved__ is False
    str(obj)
    assert obj.__resolved__ is True

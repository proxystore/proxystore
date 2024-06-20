# This module contains source code from python-lazy-object-proxy v1.10.0
# which is available under the BSD 2-Clause License included below.
#
# The following modifications to the source has been made:
#   * Replaced use of the Proxy type from lazy-object-proxy with the
#     native implementation in ProxyStore.
#   * Consolidated, updated, and/or removed tests.
#   * Formatted code and added type annotations.
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
from __future__ import annotations

import collections
import datetime
import decimal
import gc
import os
import pickle
import sys
import types
import typing
import weakref
from typing import Any

import pytest

from proxystore.factory import SimpleFactory
from proxystore.proxy import Proxy
from testing.compat import randbytes

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


def test_proxy_type_vs_instance_module() -> None:
    # The most important difference between this Proxy implementation
    # and that of lazy-object-proxy is that __module__ behaves differently
    # depending on if its the type or an instance.
    assert Proxy.__module__ == 'proxystore.proxy'
    assert Proxy.__name__ == 'Proxy'

    p = Proxy(SimpleFactory(list))
    assert isinstance(p, Proxy)
    assert p.__module__ == 'builtins'


def test_get_wrapped() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = Proxy(lambda: function1)
    assert function2.__proxy_wrapped__ == function1

    function3 = Proxy(lambda: function2)
    assert function3.__proxy_wrapped__ == function1


def test_set_wrapped() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = Proxy(lambda: function1)

    assert function2 == function1
    assert function2.__proxy_wrapped__ is function1
    assert function2.__name__ == function1.__name__

    assert function2.__qualname__ == function1.__qualname__

    function2.__proxy_wrapped__ = None  # type: ignore[assignment]

    assert not hasattr(function1, '__proxy_wrapped__')

    assert function2 == None  # noqa
    assert function2.__proxy_wrapped__ is None
    assert not hasattr(function2, '__name__')

    assert not hasattr(function2, '__qualname__')

    def function3(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2.__proxy_wrapped__ = function3

    assert function2 == function3
    assert function2.__proxy_wrapped__ == function3
    assert function2.__name__ == function3.__name__

    assert function2.__qualname__ == function3.__qualname__


def test_wrapped_attribute() -> None:
    def function1(*args, **kwargs):  # pragma: no cover
        return args, kwargs

    function2 = Proxy(lambda: function1)

    function2.variable = True

    assert hasattr(function1, 'variable')
    assert hasattr(function2, 'variable')

    assert function2.variable

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

    target: Any
    if kind == 'class':
        target = TestClass
    elif kind == 'instance':
        target = TestClass()
    elif kind == 'function':
        target = test_function
    else:
        raise AssertionError()

    wrapper = Proxy(lambda: target)

    if kind != 'instance':
        assert wrapper.__name__ == target.__name__
        assert wrapper.__qualname__ == target.__qualname__

    if kind != 'function':
        assert wrapper.__weakref__ == target.__weakref__

    assert wrapper.__module__ == target.__module__
    assert wrapper.__doc__ == target.__doc__

    if kind != 'instance':
        new_name = 'new-name'
        wrapper.__name__ = new_name
        assert wrapper.__name__ == target.__name__ == new_name

        new_ann: dict[Any, Any] = {}
        wrapper.__annotations__ = new_ann
        assert wrapper.__annotations__ == target.__annotations__ == new_ann

    new_module = 'new-module'
    wrapper.__module__ = new_module
    assert wrapper.__module__ == target.__module__ == new_module

    new_doc = 'new-doc'
    wrapper.__doc__ = new_doc
    assert wrapper.__doc__ == target.__doc__ == new_doc


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason='Requires Python 3.10 or later',
)
@pytest.mark.parametrize('kind', ('class', 'function'))
def test_annotations(kind: str) -> None:  # pragma: >=3.10 cover
    class TestClass:
        """Test class."""

        pass

    def test_function() -> None:  # pragma: no cover
        """Test function."""
        pass

    target: Any
    if kind == 'class':
        target = TestClass
    elif kind == 'function':
        target = test_function
    else:
        raise AssertionError()

    wrapper = Proxy(lambda: target)

    assert wrapper.__annotations__ == target.__annotations__

    new_ann: dict[Any, Any] = {}
    wrapper.__annotations__ = new_ann
    assert wrapper.__annotations__ == target.__annotations__ == new_ann


def test_isinstance_class_comparision() -> None:
    # Class
    target = objects.Target
    wrapper = Proxy(lambda: target)
    assert wrapper.__class__ is target.__class__
    assert isinstance(wrapper, type(target))

    # Instance
    target = objects.Target()
    wrapper = Proxy(lambda: target)
    assert wrapper.__class__ is target.__class__
    assert isinstance(wrapper, objects.Target)
    assert isinstance(wrapper, objects.TargetBaseClass)

    # Functions
    target = objects.target
    wrapper = Proxy(lambda: target)
    assert wrapper.__class__ is target.__class__
    assert isinstance(wrapper, type(target))


def test_revert_class_proxying() -> None:
    class ProxyWithOldStyleIsInstance(Proxy[Any]):
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


def test_dir() -> None:
    # Class
    target = objects.Target
    wrapper = Proxy(lambda: target)
    assert dir(wrapper) == dir(target)

    # Instance
    target = objects.Target()
    wrapper = Proxy(lambda: target)
    assert dir(wrapper) == dir(target)

    # Function
    target = objects.target
    wrapper = Proxy(lambda: target)
    assert dir(wrapper) == dir(target)


def test_vars() -> None:
    # Class
    target = objects.Target
    wrapper = Proxy(lambda: target)
    assert vars(wrapper) == vars(target)

    # Instance
    target = objects.Target()
    wrapper = Proxy(lambda: target)
    assert vars(wrapper) == vars(target)

    # Functions
    target = objects.target
    wrapper = Proxy(lambda: target)
    assert vars(wrapper) == vars(target)


def test_function_invocation() -> None:
    def function(*args, **kwargs):
        return args, kwargs

    _args, _kwargs = (), {}  # type: ignore
    wrapper = Proxy(lambda: function)
    result = wrapper()
    assert result == (_args, _kwargs)

    _args, _kwargs = (1, 2), {}  # type: ignore
    wrapper = Proxy(lambda: function)
    result = wrapper(*_args)
    assert result == (_args, _kwargs)

    _args, _kwargs = (), {'one': 1, 'two': 2}
    wrapper = Proxy(lambda: function)
    result = wrapper(**_kwargs)
    assert result == (_args, _kwargs)

    _args, _kwargs = (1, 2), {'one': 1, 'two': 2}  # type: ignore
    wrapper = Proxy(lambda: function)
    result = wrapper(*_args, **_kwargs)
    assert result == (_args, _kwargs)


def test_instancemethod_invocation() -> None:
    class TestClass:
        def function(self, *args, **kwargs):
            return args, kwargs

    _args, _kwargs = (), {}  # type: ignore
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper() == (_args, _kwargs)

    _args, _kwargs = (1, 2), {}  # type: ignore
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(*_args) == (_args, _kwargs)

    _args, _kwargs = (), {'one': 1, 'two': 2}
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(**_kwargs) == (_args, _kwargs)

    _args, _kwargs = (1, 2), {'one': 1, 'two': 2}  # type: ignore
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(*_args, **_kwargs) == (_args, _kwargs)


def test_classmethod_invocation() -> None:
    class TestClass:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    _args, _kwargs = (), {}  # type: ignore
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper() == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper() == (_args, _kwargs)

    _args, _kwargs = (1, 2), {}  # type: ignore
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper(*_args) == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(*_args) == (_args, _kwargs)

    _args, _kwargs = (), {'one': 1, 'two': 2}
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper(**_kwargs) == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(**_kwargs) == (_args, _kwargs)

    _args, _kwargs = (1, 2), {'one': 1, 'two': 2}  # type: ignore
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper(*_args, **_kwargs) == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(*_args, **_kwargs) == (_args, _kwargs)


def test_staticmethod_invocation() -> None:
    class TestClass:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    _args, _kwargs = (), {}  # type: ignore
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper() == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper() == (_args, _kwargs)

    _args, _kwargs = (1, 2), {}  # type: ignore
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper(*_args) == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(*_args) == (_args, _kwargs)

    _args, _kwargs = (), {'one': 1, 'two': 2}
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper(**_kwargs) == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(**_kwargs) == (_args, _kwargs)

    _args, _kwargs = (1, 2), {'one': 1, 'two': 2}  # type: ignore
    wrapper = Proxy(lambda: TestClass.function)
    assert wrapper(*_args, **_kwargs) == (_args, _kwargs)
    wrapper = Proxy(lambda: TestClass().function)
    assert wrapper(*_args, **_kwargs) == (_args, _kwargs)


def test_iteration() -> None:
    # Simple iteration
    items = [1, 2]
    wrapper = Proxy(lambda: items)
    result = [x for x in wrapper]  # noqa: C416
    assert result == items

    # Not iterable error propagation
    with pytest.raises(TypeError):
        iter(Proxy(lambda: 1))

    class TestClass:
        value = 1

        def __next__(self) -> int:
            return self.value

    # Manual iteration with next
    wrapper_next = Proxy(lambda: TestClass())
    assert next(wrapper_next) == 1

    # Construct iterator directly
    iter(Proxy(lambda: [1, 2]))

    # Not iterable type error
    with pytest.raises(TypeError):
        iter(Proxy(lambda: 1))


def test_context_manager() -> None:
    class TestClass:
        def __enter__(self):
            return self

        def __exit__(*args, **kwargs):
            return

    with Proxy(lambda: TestClass()):
        pass


def test_str() -> None:
    value1 = Proxy(lambda: 10)
    assert str(value1) == str(10)

    value2 = Proxy(lambda: (10,))
    assert str(value2) == str((10,))

    value3 = Proxy(lambda: [10])
    assert str(value3) == str([10])

    value4 = Proxy(lambda: {10: 10})
    assert str(value4) == str({10: 10})


def test_str_format() -> None:
    instance = 'abcd'
    proxy = Proxy(lambda: instance)  # pragma: no cover
    assert format(instance, ''), format(proxy == '')


def test_repr() -> None:
    class TestClass:
        pass

    value1 = Proxy(lambda: TestClass())
    str(value1)
    representation = repr(value1)
    assert 'Proxy at' in representation
    assert 'lambda' in representation
    assert 'TestClass' in representation

    # Validate calling repr does not invoke the factory
    consumed = []
    value2 = Proxy(lambda: consumed.append(1))  # pragma: no cover
    _repr = repr(value2)
    assert not consumed


def test_derived_new() -> None:
    class DerivedObjectProxy(Proxy[Any]):
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

    class DerivedObjectProxy(Proxy[Any]):
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

    class DerivedObjectProxy(Proxy[Any]):
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

    proxy = Proxy(lambda: function)  # pragma: no cover

    assert hasattr(proxy, '__getattr__')
    assert hasattr(proxy, '__setattr__')
    assert hasattr(proxy, '__delattr__')


def test_override_getattr() -> None:
    def function():  # pragma: no cover
        pass

    accessed = []

    class DerivedObjectProxy(Proxy[Any]):
        def __getattr__(self, name):
            accessed.append(name)
            try:
                __getattr__ = super().__getattr__
            except AttributeError as e:  # pragma: no cover
                raise RuntimeError(str(e)) from e
            return __getattr__(name)

    function.attribute = 1  # type: ignore[attr-defined]

    proxy = DerivedObjectProxy(lambda: function)

    assert proxy.attribute == 1

    assert 'attribute' in accessed


def test_callable_proxy_hasattr_call() -> None:
    proxy = Proxy(lambda: None)  # pragma: no cover
    # This check is always true because Proxy defines __call__.
    assert callable(proxy)


def test_readonly() -> None:
    proxy = Proxy(lambda: object)
    assert proxy.__qualname__ == 'object'


def test_del_wrapped() -> None:
    foo = object()
    called = []

    def make_foo():
        called.append(1)
        return foo

    proxy = Proxy(make_foo)
    str(proxy)
    assert called == [1]
    assert proxy.__proxy_wrapped__ is foo
    del proxy.__proxy_wrapped__
    str(proxy)
    assert called == [1, 1]


def test_raise_attribute_error() -> None:
    def foo():
        raise AttributeError('boom!')

    proxy = Proxy(foo)
    pytest.raises(AttributeError, str, proxy)
    pytest.raises(AttributeError, lambda: proxy.__proxy_wrapped__)
    assert proxy.__proxy_factory__ is foo


def test_patching_the_factory() -> None:
    def foo():
        raise AttributeError('boom!')

    proxy = Proxy(foo)
    pytest.raises(AttributeError, lambda: proxy.__proxy_wrapped__)
    assert proxy.__proxy_factory__ is foo

    proxy.__proxy_factory__ = lambda: foo
    pytest.raises(AttributeError, proxy)
    assert proxy.__proxy_wrapped__ is foo


def test_deleting_the_factory() -> None:
    proxy = Proxy(lambda: 'value')  # pragma: no cover
    assert proxy.__proxy_factory__ is not None
    proxy.__proxy_factory__ = None  # type: ignore[assignment]
    assert proxy.__proxy_factory__ is None
    del proxy.__proxy_factory__
    pytest.raises(ValueError, str, proxy)


def test_new() -> None:
    a = Proxy.__new__(Proxy)
    b = Proxy.__new__(Proxy)
    pytest.raises(ValueError, lambda: a + b)
    pytest.raises(ValueError, lambda: a.__proxy_wrapped__)


def test_set_wrapped_via_new() -> None:
    obj = Proxy.__new__(Proxy)
    obj.__proxy_wrapped__ = 1
    assert str(obj) == '1'
    assert obj + 1 == 2


def test_set_wrapped_regular() -> None:
    obj = Proxy(lambda: 'value')  # pragma: no cover
    obj.__proxy_wrapped__ = 1  # type: ignore[assignment]
    assert str(obj) == '1'
    assert obj + 1 == 2


@pytest.mark.parametrize(
    'obj',
    (
        1,
        ['b', 'c'],
        {'d': 'e'},
        datetime.date(2015, 5, 1),
        decimal.Decimal('1.2'),
    ),
)
def test_pickling(obj):
    for level in range(pickle.HIGHEST_PROTOCOL + 1):
        proxy = Proxy(SimpleFactory(obj))

        try:
            dump = pickle.dumps(proxy, protocol=level)
            result = pickle.loads(dump)
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                f'Failed to pickle {obj} with pickle protocol {level}:',
            ) from e

        assert obj == result


def bytes_factory() -> bytes:
    return randbytes(1_000)


def test_pickling_factory_only():
    # The actually size of the pickled proxy is around 130, but pickle
    # protocols could be added in the futures changing this slightly so
    # this value includes some buffer. Nonetheless it is much less than
    # the 1000 byte target object returned by bytes_factory.
    max_pickle_size = 200

    for level in range(pickle.HIGHEST_PROTOCOL + 1):
        proxy = Proxy(bytes_factory)
        assert isinstance(proxy, bytes)

        try:
            dump = pickle.dumps(proxy, protocol=level)
            assert sys.getsizeof(dump) < max_pickle_size
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                f'Failed to pickle with pickle protocol {level}:',
            ) from e


def test_garbage_collection() -> None:
    leaky = lambda: 'foobar'  # noqa
    proxy = Proxy(leaky)
    leaky.leak = proxy  # type: ignore[attr-defined]
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
        str(Proxy(lambda: obj))
    assert count == sys.getrefcount(obj)


def test_subclassing_with_local_attr() -> None:
    class LazyProxy(Proxy[Any]):
        name = None

        def __init__(self, func, **lazy_attr):
            super().__init__(func)
            for attr, val in lazy_attr.items():
                setattr(self, attr, val)

    called = []
    proxy = LazyProxy(
        lambda: called.append(1),  # pragma: no cover
        name='bar',
    )
    assert proxy.name == 'bar'
    assert not called


def test_subclassing_dynamic_with_local_attr() -> None:
    class LazyProxy(Proxy[Any]):
        def __init__(self, func, **lazy_attr):
            super().__init__(func)
            for attr, val in lazy_attr.items():
                object.__setattr__(self, attr, val)

    called = []
    proxy = LazyProxy(
        lambda: called.append(1),  # pragma: no cover
        name='bar',
    )
    assert proxy.name == 'bar'
    assert not called


class FSPathMock:
    def __fspath__(self):
        return '/foobar'


def test_fspath() -> None:
    assert os.fspath(Proxy(lambda: '/foobar')) == '/foobar'
    assert os.fspath(Proxy(FSPathMock)) == '/foobar'
    with pytest.raises(TypeError) as excinfo:
        os.fspath(Proxy(lambda: None))
    assert (
        '__fspath__() to return str or bytes, not NoneType'
        in excinfo.value.args[0]
    )


def test_fspath_method() -> None:
    assert Proxy(FSPathMock).__fspath__() == '/foobar'


def test_resolved_new() -> None:
    obj = Proxy.__new__(Proxy)
    assert not obj.__proxy_resolved__


def test_resolved() -> None:
    obj = Proxy(lambda: None)
    assert not obj.__proxy_resolved__
    assert obj.__proxy_wrapped__ is None
    assert obj.__proxy_resolved__


def test_resolved_str() -> None:
    obj = Proxy(lambda: None)
    assert not obj.__proxy_resolved__
    str(obj)
    assert obj.__proxy_resolved__


def test_preset_target() -> None:
    target = 'value'
    proxy = Proxy(lambda: target, target=target)  # pragma: no cover
    assert proxy.__proxy_resolved__


def test_precompute_defaults() -> None:
    target = 'value'

    proxy = Proxy(
        lambda: target,  # pragma: no cover
        cache_defaults=True,
        target=target,
    )
    del proxy.__proxy_wrapped__

    assert not proxy.__proxy_resolved__
    assert hash(proxy) == hash(target)
    assert not proxy.__proxy_resolved__
    assert isinstance(proxy, str)
    assert not proxy.__proxy_resolved__
    assert not isinstance(proxy, list)
    assert not proxy.__proxy_resolved__


def test_precompute_unhashable() -> None:
    target = [1, 2, 3]

    proxy = Proxy(
        lambda: target,  # pragma: no cover
        cache_defaults=True,
        target=target,
    )
    del proxy.__proxy_wrapped__

    assert not proxy.__proxy_resolved__
    try:
        hash(proxy)
    except TypeError:
        pass
    assert not proxy.__proxy_resolved__
    assert isinstance(proxy, list)
    assert not proxy.__proxy_resolved__


def test_generic_alias_type_checking() -> None:
    my_dict_proxy = Proxy(lambda: {'a': 1, 'b': 2})
    assert isinstance(my_dict_proxy, dict)
    assert not isinstance(my_dict_proxy, typing.Mapping)
    assert isinstance(my_dict_proxy, collections.abc.Mapping)

    my_list_proxy = Proxy(lambda: [1, 2, 3])
    assert isinstance(my_list_proxy, list)
    assert not isinstance(my_list_proxy, typing.Sequence)
    assert isinstance(my_list_proxy, collections.abc.Sequence)

from __future__ import annotations

import sys
from typing import Any
from typing import Callable
from typing import Generic
from typing import overload
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

T = TypeVar('T')
P = ParamSpec('P')


class ProxyProperty(Generic[T]):
    """Custom property with different behaviour for classes and instances.

    Example:
        This class is intended to be initialized via the
        `proxy_property()` decorator.
        ```python
        class Foo:
            @proxy_property(default='class')
            def bar(self) -> str:
                return 'instance'

        assert Foo.bar == 'class'
        assert Foo().bar == 'instance'
        ```

    This implementation is based on the pure Python `Property` implementation
    in the
    [Descriptor Guide](https://docs.python.org/3/howto/descriptor.html#properties){target=_blank}.

    SQLAlchemy has a similar construct called
    [hybrid_property](https://github.com/sqlalchemy/sqlalchemy/blob/3b520e758a715cf817075e4a90ae1b5813ffadd3/lib/sqlalchemy/ext/hybrid.py#L968)
    with more advanced features.
    """

    def __init__(
        self,
        fget: Callable[[Any], T],
        fset: Callable[[Any, T], None] | None = None,
        fdel: Callable[[Any], None] | None = None,
        default: T | None = None,
        doc: str | None = None,
    ):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.default = default
        self.__doc__ = doc if doc is not None else fget.__doc__
        self._name = ''

    def __set_name__(self, owner: Any, name: str) -> None:
        self._name = name

    def __get__(self, obj: Any, objtype: Any = None) -> T:
        if obj is None and self.default is None:
            raise AttributeError(
                f'property {self._name!r} has no default value',
            )
        elif obj is None and self.default is not None:
            return self.default
        else:
            return self.fget(obj)

    def __set__(self, obj: Any, value: T) -> None:
        if self.fset is None:
            raise AttributeError(
                f'property {self._name!r} of {type(obj).__name__!r} '
                'object has no setter',
            )
        self.fset(obj, value)

    def __delete__(self, obj: Any) -> None:
        if self.fdel is None:
            raise AttributeError(
                f'property {self._name!r} of {type(obj).__name__!r} '
                'object has no deleter',
            )
        self.fdel(obj)

    # https://github.com/python/mypy/issues/3004#issuecomment-1807107181
    def setter(
        self,
        fset: Callable[[Any, T], None],
    ) -> Callable[[Any, T], None]:
        self.fset = fset
        return fset

    # https://github.com/python/mypy/issues/3004#issuecomment-1807107181
    def deleter(self, fdel: Callable[[Any], None]) -> Callable[[Any], None]:
        self.fdel = fdel
        return fdel


@overload
def proxy_property(
    function: Callable[P, T],
    *,
    default: T | None = ...,
) -> ProxyProperty[T]: ...


@overload
def proxy_property(
    function: None = ...,
    *,
    default: T | None = ...,
) -> Callable[[Callable[P, T]], ProxyProperty[T]]: ...


def proxy_property(
    function: Callable[P, T] | None = None,
    *,
    default: T | None = None,
) -> Any:
    # ProxyProperty[T] | Callable[[Callable[P, T]], ProxyProperty[T]]:
    if function is not None:
        return ProxyProperty(function, default=default)
    else:

        def _wrapper(_function: Callable[P, T]) -> ProxyProperty[T]:
            return ProxyProperty(_function, default=default)

        return _wrapper

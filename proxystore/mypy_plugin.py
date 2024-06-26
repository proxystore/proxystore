"""ProxyStore mypy plugin.

The [`Proxy`][proxystore.proxy.Proxy] class behaves poorly with
[mypy](https://mypy-lang.org/) out of the box. Consider the following example.
Mypy can determine that `proxy` is of type `Proxy[Foo]` but is unable to
determine the correct types when accessing an attribute of `Foo` indirectly
via the [`Proxy`][proxystore.proxy.Proxy] instance.

```python linenums="1"
from proxystore.proxy import Proxy

class Foo:
    def bar(self) -> int:
        return 42

def factory() -> Foo:
    return Foo()

proxy = Proxy(factory)
reveal_type(proxy)  # Revealed type is "Proxy[Foo]"

bar = proxy.bar()
reveal_type(bar)  # Revealed type is "Any"
```

ProxyStore (v0.6.5 and later) comes with an optional mypy plugin which can fix
these type resolution limitations. With the mypy plugin enabled, we get the
correct type.

```python linenums="1"
proxy = Proxy(factory)
reveal_type(proxy)  # Revealed type is "Proxy[Foo]"

bar = proxy.bar()
reveal_type(bar)  # Revealed type is "int"
```

Enable the plugin by adding `proxystore.mypy_plugin` to the list of plugins
in your
[mypy config file](https://mypy.readthedocs.io/en/latest/config_file.html){target=_blank}.

* `pyproject.toml`
  ```toml
  [tools.mypy]
  plugins = ["proxystore.mypy_plugin"]
  ```
* `mypy.ini` and `setup.cfg`
  ```ini
  [mypy]
  plugins = proxystore.mypy_plugin
  ```
"""

from __future__ import annotations

import functools
import sys
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from mypy.errorcodes import ATTR_DEFINED
from mypy.errorcodes import UNION_ATTR
from mypy.messages import format_type
from mypy.options import Options
from mypy.plugin import AttributeContext
from mypy.plugin import Plugin
from mypy.subtypes import find_member
from mypy.types import AnyType
from mypy.types import get_proper_type
from mypy.types import Instance
from mypy.types import Type
from mypy.types import TypeOfAny
from mypy.types import TypeVarType
from mypy.types import UnionType

P = ParamSpec('P')
T = TypeVar('T')

PROXY_TYPES = (
    'proxystore.proxy.Proxy',
    'proxystore.store.ref.BaseRefProxy',
    'proxystore.store.ref.OwnedProxy',
    'proxystore.store.ref.RefProxy',
    'proxystore.store.ref.RefMutProxy',
)


class ProxyStoreMypyPlugin(Plugin):  # noqa: D101
    def __init__(self, options: Options) -> None:
        super().__init__(options)

    def get_attribute_hook(  # noqa: D102
        self,
        fullname: str,
    ) -> Callable[[AttributeContext], Type] | None:
        sym = self.lookup_fully_qualified(fullname)
        # Note the dot at the end of the name check to make sure this
        # is an attribute access on Proxy type.
        if sym is None and any(
            fullname.startswith(f'{name}.') for name in PROXY_TYPES
        ):
            _, attr = fullname.rsplit('.', 1)
            return functools.partial(proxy_attribute_access, attr=attr)
        return None


def _assertion_fallback(function: Callable[P, Type]) -> Callable[P, Type]:
    # Decorator which catches AssertionErrors and returns AnyType
    # to indicate that the plugin does not know how to handle that case
    # and will default back to Any.
    # https://github.com/dry-python/returns/blob/dda187d78fe405d7d1234ffaffc99d8264f854dc/returns/contrib/mypy/_typeops/fallback.py
    @functools.wraps(function)
    def decorator(*args: P.args, **kwargs: P.kwargs) -> Type:
        try:
            return function(*args, **kwargs)
        except AssertionError:
            return AnyType(TypeOfAny.implementation_artifact)

    return decorator


def _proxy_attribute_access(
    instance: Type,
    attr: str,
    ctx: AttributeContext,
) -> Type:
    instance = get_proper_type(instance)
    if not isinstance(instance, Instance):
        return ctx.default_attr_type

    # Instance is not a Proxy type so handle it normally. This case happens
    # when checking the T branch of a type annotated as Proxy[T] | T.
    fullname = instance.type.fullname
    if not any(fullname.startswith(name) for name in PROXY_TYPES):
        member = find_member(attr, instance, instance)
        if member is None:
            return ctx.default_attr_type
        else:
            return member

    # After the above check, we know instance is a Proxy type and Proxy
    # types are generic with one generic type.
    assert len(instance.args) == 1
    generic_type = get_proper_type(instance.args[0])

    if isinstance(generic_type, TypeVarType):
        # We have an unbound Proxy[T] so return the default type.
        return ctx.default_attr_type
    elif isinstance(generic_type, Instance):
        # We have a bound Proxy[T] so lookup the attr on T.
        member = find_member(attr, generic_type, generic_type)
        if member is None:
            type_ = format_type(instance, ctx.api.options)
            code = ATTR_DEFINED
            if isinstance(ctx.type, UnionType):
                union = format_type(ctx.type, ctx.api.options)
                type_ = f'Item {type_} of {union}'
                code = UNION_ATTR
            ctx.api.fail(
                f'{type_} has no attribute "{attr}"',
                ctx.context,
                code=code,
            )
            return ctx.default_attr_type
        else:
            return member
    else:
        return ctx.default_attr_type


@_assertion_fallback
def proxy_attribute_access(ctx: AttributeContext, *, attr: str) -> Type:  # noqa: D103
    if isinstance(ctx.type, UnionType):
        resolved = tuple(
            _proxy_attribute_access(instance, attr, ctx)
            for instance in ctx.type.items
        )
        return UnionType(resolved)
    elif isinstance(ctx.type, Instance):
        return _proxy_attribute_access(ctx.type, attr, ctx)
    else:
        return ctx.default_attr_type


def plugin(version: str) -> type[ProxyStoreMypyPlugin]:  # noqa: D103
    return ProxyStoreMypyPlugin

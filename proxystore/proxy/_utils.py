from __future__ import annotations

from typing import Any

from proxystore.proxy._property import proxy_property


async def _do_await(obj):  # type: ignore[no-untyped-def] # pragma: no cover
    return await obj


def _do_yield_from(gen):  # type: ignore[no-untyped-def] # pragma: no cover
    return (yield from gen)


def as_metaclass(meta: Any, *bases: Any) -> Any:
    return meta('ProxyMetaClass', bases, {})


class _ProxyMethods:
    # We use properties to override the values of __module__ and
    # __doc__. If we add these in ObjectProxy, the derived class
    # __dict__ will still be setup to have string variants of these
    # attributes and the rules of descriptors means that they appear to
    # take precedence over the properties in the base class. To avoid
    # that, we copy the properties into the derived class type itself
    # via a meta class. In that way the properties will always take
    # precedence.
    __proxy_wrapped__: Any

    # The Proxy class is re-exported from proxystore/proxy/__init__.py
    # and this module is hidden from the docs so we set the Proxy class's
    # module to proxystore.proxy.
    @proxy_property(default='proxystore.proxy')
    def __module__(self) -> str:  # type: ignore[override]
        return self.__proxy_wrapped__.__module__

    @__module__.setter
    def __module__set(self, value: str) -> None:
        self.__proxy_wrapped__.__module__ = value

    @proxy_property(default='<Proxy Placeholder Docstring>')
    def __doc__(self) -> str:  # type: ignore[override]
        return self.__proxy_wrapped__.__doc__

    @__doc__.setter
    def __doc__set(self, value: str) -> None:
        self.__proxy_wrapped__.__doc__ = value

    @property
    def __annotations__(self) -> dict[str, Any]:
        return self.__proxy_wrapped__.__annotations__

    @__annotations__.setter
    def __annotations__(self, value: dict[str, Any]) -> None:
        self.__proxy_wrapped__.__annotations__ = value

    # We similar use a property for __dict__. We need __dict__ to be
    # explicit to ensure that vars() works as expected.

    @property
    def __dict__(self) -> dict[str, Any]:  # type: ignore[override]
        return self.__proxy_wrapped__.__dict__

    # Need to also propagate the special __weakref__ attribute for case
    # where decorating classes which will define this. If do not define
    # it and use a function like inspect.getmembers() on a decorator
    # class it will fail. This can't be in the derived classes.

    @property
    def __weakref__(self) -> Any:
        return self.__proxy_wrapped__.__weakref__


class ProxyMetaType(type):
    def __new__(
        cls,
        name: str,
        bases: tuple[Any, ...],
        dictionary: dict[Any, Any],
    ) -> Any:
        # Copy our special properties into the class so that they
        # always take precedence over attributes of the same name added
        # during construction of a derived class. This is to save
        # duplicating the implementation for them in all derived classes.

        dictionary.update(vars(_ProxyMethods))
        # dictionary.pop('__dict__')

        return type.__new__(cls, name, bases, dictionary)

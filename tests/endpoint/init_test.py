from __future__ import annotations

import inspect
import typing

import proxystore.endpoint


def test_all_exported() -> None:
    for name in proxystore.endpoint.__all__:
        assert hasattr(proxystore.endpoint, name)
    assert sorted(proxystore.endpoint.__all__) == sorted(
        set(proxystore.endpoint.__all__),
    )


def _referenced_modules(obj: object) -> set[str]:
    # Modules of the types in the signatures of the public methods.
    functions = (
        [obj]
        if inspect.isfunction(obj)
        else [
            member
            for name, member in inspect.getmembers(obj, inspect.isfunction)
            if not name.startswith('_') or name == '__init__'
        ]
    )
    modules: set[str] = set()
    for function in functions:
        try:
            hints = typing.get_type_hints(function)
        except NameError:  # pragma: no cover
            continue
        for hint in hints.values():
            for arg in (hint, *typing.get_args(hint)):
                module = getattr(arg, '__module__', '')
                if module.startswith('proxystore.endpoint'):
                    modules.add(module)
    return modules


def test_public_signatures_use_exported_types() -> None:
    exported = {
        getattr(proxystore.endpoint, name)
        for name in proxystore.endpoint.__all__
    }
    public_modules = {obj.__module__ for obj in exported}
    for name in ('Endpoint', 'EndpointClient', 'EndpointDir'):
        obj = getattr(proxystore.endpoint, name)
        assert _referenced_modules(obj) <= public_modules, name

"""Utilities for managing reference proxy scopes and lifetimes."""
from __future__ import annotations

import sys
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from proxystore.store.ref import RefMutProxy
from proxystore.store.ref import RefProxy

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec


@runtime_checkable
class FutureWithCallback(Protocol):
    """Protocol for Future objects that support callbacks."""

    def add_done_callback(
        self,
        callback: Callable[[FutureWithCallback], Any],
    ) -> None:
        ...


FutureT = TypeVar('FutureT', bound=FutureWithCallback)
P = ParamSpec('P')


def mark_refs_out_of_scope(
    *refs: RefProxy[Any] | RefMutProxy[Any],
) -> None:
    """Mark proxy references as out of scope.

    This (1) decrements the reference count in the owner proxy, (2)
    marks the reference proxy invalid, and (3) removes the reference to the
    owner from the reference proxy so the reference proxy will not prevent
    the owned proxy from being garbage collected.

    Args:
        refs: Reference proxies to mark out of scope.

    Raises:
        RuntimeError: if a reference proxy does not have a reference to its
            owner.
    """
    for ref in refs:
        owner = object.__getattribute__(ref, '__owner__')
        if owner is None:
            raise RuntimeError(
                f'Cannot mark {owner!r} as out of scope because it has '
                'no reference to its owner.',
            )

        if isinstance(ref, RefProxy):
            ref_count = object.__getattribute__(owner, '__ref_count__')
            object.__setattr__(owner, '__ref_count__', ref_count - 1)
        elif isinstance(ref, RefMutProxy):
            ref_count = object.__getattribute__(owner, '__ref_mut_count__')
            object.__setattr__(owner, '__ref_mut_count__', ref_count - 1)
        else:
            raise AssertionError('Unreachable.')

        # Remove ref's reference to owner so it no longer keeps owner alive
        object.__setattr__(ref, '__owner__', None)
        # Mark ref as invalid in case the user tries to use it after it
        # has already "gone out of scope."
        object.__setattr__(ref, '__valid__', False)


def _make_out_of_scope_callback(
    refs: Iterable[RefProxy[Any] | RefMutProxy[Any]],
) -> Callable[[FutureWithCallback], None]:
    def _out_of_scope_callback(_fut: FutureWithCallback) -> None:
        mark_refs_out_of_scope(*refs)

    return _out_of_scope_callback


def submit(
    submit_func: Callable[P, FutureT],
    *,
    args: P.args = (),
    kwargs: P.kwargs | None = None,
    register_custom_refs: Iterable[Any] = (),
) -> FutureT:
    """Shim around function executor for managing reference proxy scopes.

    When invoking a remote function, such as via a
    [`ProcessPoolExecutor`][concurrent.futures.ProcessPoolExecutor]{target=_blank}
    or a FaaS system, on a proxy reference, the owner proxy will not know when
    the proxy references go out of scope on the remote process. This function
    will register a callback to the future returned by the function
    invocation method (`submit_func`) that will mark all proxy references in
    `args` and `kwargs` as out of scope once the future completes.

    Args:
        submit_func: Function with submits a function with args and kwargs to
            be executed (e.g.,
            [`Executor.submit()`][concurrent.futures.Executor.submit]{target=_blank}).
        args: Positional arguments to pass to `submit_func`. Any proxy
            references in `args` will be registered to the scope cleanup
            callback.
        kwargs: Keyword arguments to pass to `submit_func`. Any proxy
            references in the values of `kwargs` will be registered to the
            scope cleanup callback.
        register_custom_refs: Iterable of additional proxy references to
            register to the scope cleanup callback. This is helpful if `args`
            or `kwargs` contain complex data structures containing more proxy
            references.
    """
    kwargs = {} if kwargs is None else kwargs

    refs: set[RefProxy[Any] | RefMutProxy[Any]] = {
        ref
        for ref in register_custom_refs
        if isinstance(ref, (RefProxy, RefMutProxy))
    }
    refs.update(
        arg for arg in args if isinstance(arg, (RefProxy, RefMutProxy))
    )
    refs.update(
        kwarg
        for kwarg in kwargs.values()
        if isinstance(kwarg, (RefProxy, RefMutProxy))
    )

    fut = submit_func(*args, **kwargs)

    fut.add_done_callback(_make_out_of_scope_callback(refs))

    return fut

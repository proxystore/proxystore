from __future__ import annotations

import importlib
import pkgutil

import proxystore.endpoint

_PUBLIC_MODULES = {
    'proxystore.endpoint.client',
    'proxystore.endpoint.exceptions',
    'proxystore.endpoint.warnings',
}
_INTERNAL_WARNING = 'internal implementation detail'


def _modules() -> list[str]:
    return [
        info.name
        for info in pkgutil.walk_packages(
            proxystore.endpoint.__path__,
            prefix='proxystore.endpoint.',
        )
    ]


def test_package_does_not_export_names() -> None:
    assert not hasattr(proxystore.endpoint, '__all__')


def test_public_modules_are_documented() -> None:
    doc = proxystore.endpoint.__doc__
    assert doc is not None
    for name in _PUBLIC_MODULES:
        assert f'[{name}]' in doc, name


def test_internal_modules_are_marked() -> None:
    modules = _modules()
    assert set(modules) >= _PUBLIC_MODULES
    for name in modules:
        doc = ' '.join((importlib.import_module(name).__doc__ or '').split())
        internal = _INTERNAL_WARNING in doc
        assert internal == (name not in _PUBLIC_MODULES), name

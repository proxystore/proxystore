from __future__ import annotations

import proxystore.endpoint

_PUBLIC_MODULES = {
    'proxystore.endpoint.exceptions',
    'proxystore.endpoint.warnings',
}


def test_package_does_not_export_names() -> None:
    assert not hasattr(proxystore.endpoint, '__all__')


def test_package_is_marked_internal() -> None:
    doc = proxystore.endpoint.__doc__
    assert doc is not None
    assert 'internal implementation detail' in ' '.join(doc.split())
    # The exceptions to the warning are named in the package documentation
    for name in _PUBLIC_MODULES:
        assert f'[{name}]' in doc, name

from __future__ import annotations

import proxystore.endpoint


def test_package_does_not_export_names() -> None:
    assert not hasattr(proxystore.endpoint, '__all__')


def test_package_is_marked_internal() -> None:
    doc = proxystore.endpoint.__doc__
    assert doc is not None
    assert 'internal implementation detail' in ' '.join(doc.split())

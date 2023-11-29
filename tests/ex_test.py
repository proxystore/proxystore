from __future__ import annotations


def test_import_proxystore_ex() -> None:
    import proxystore.ex

    del proxystore.ex


def test_getattr_proxystore_ex() -> None:
    from proxystore.ex import __version__

    del __version__

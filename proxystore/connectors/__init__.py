"""Connector implementations.

A [`Connector`][proxystore.connectors.protocols.Connector] is an interface
to a mediated communication channel or object store. Connectors operate on
low-level bytes and are used by the [`Store`][proxystore.store.base.Store]
to store and get serialized Python objects.

Third-party code can provide custom connectors by implementing the
[`Connector`][proxystore.connectors.protocols.Connector] protocol. (Note:
because [`Connector`][proxystore.connectors.protocols.Connector] is a
[`Protocol`][typing.Protocol], custom connectors do not need
to inherit from [`Connector`][proxystore.connectors.protocols.Connector].)

Example:
    ```python
    from proxystore.connectors.file import FileConnector

    connector = FileConnector('./data-store')
    key = connector.put(b'hello')
    connector.get(key)
    >>> b'hello'
    connector.evict(key)
    connector.exists(key)
    >>> False
    connector.close()
    ```

Tip:
    All of the [`Connector`][proxystore.connectors.protocols.Connector]
    implementations in this module can be used as context managers.
    Context manager support is not a required component of the
    [`Connector`][proxystore.connectors.protocols.Connector] protocol. It is
    simply provided for convenience with the native implementations.
    ```python
    from proxystore.connectors.file import FileConnector

    with FileConnector('./data-store') as connector:
        # connector.close() will be automatically called when the
        # context manager is exited
        ...
    ```
"""

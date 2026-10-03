# Versioning and Compatibility

ProxyStore follows [semantic versioning](https://semver.org){target=_blank}.
Given a version `MAJOR.MINOR.PATCH`:

* **Major** releases may make breaking changes to the public API or to the
  format of objects exchanged between processes (e.g., pickled proxies).
* **Minor** releases add features in a backwards compatible manner.
* **Patch** releases fix bugs in a backwards compatible manner.

## Public API

The public API is everything documented in the
[API Reference](api/index.md) and the [CLI Reference](api/cli.md),
except for:

* Names prefixed with an underscore (e.g., `proxystore._compat` or
  `Store._set()`).
* Modules documented as internal implementation details. These are used by
  ProxyStore itself and may change between releases without notice:
    * [`proxystore.endpoint`][proxystore.endpoint]: use endpoints via the
      [`proxystore-endpoint`](api/cli.md#proxystore-endpoint) CLI and the
      [`EndpointConnector`][proxystore.connectors.endpoint.EndpointConnector].
    * [`proxystore.globus`][proxystore.globus]: use the
      [`proxystore-globus-auth`](api/cli.md#proxystore-globus-auth) CLI and
      the [`GlobusConnector`][proxystore.connectors.globus.GlobusConnector].
    * [`proxystore.utils`][proxystore.utils].
    * [`proxystore.store.cache`][proxystore.store.cache].
    * The server functions and classes of
      [`proxystore.connectors.zmq`][proxystore.connectors.zmq] (the
      [`ZeroMQConnector`][proxystore.connectors.zmq.ZeroMQConnector] is
      public).

Deprecated features emit a [`DeprecationWarning`][DeprecationWarning] for
at least one minor release before they are removed in the next major
release.

## Compatibility Between Versions

Processes using different versions of ProxyStore with the same major
version can exchange proxies, store configurations, and stream events.
For example, a proxy created with ProxyStore 2.3 can be resolved by a
process using ProxyStore 2.0, and vice versa.
Fields added by a newer version are ignored by an older version with a
[`VersionMismatchWarning`][proxystore.warnings.VersionMismatchWarning].
Proxies are not compatible between major versions.

The configuration files and protocols of ProxyStore Endpoints are versioned
separately (see the
[Endpoints Guide](guides/endpoints.md#version-compatibility)).

## Python Versions

Each Python version is supported until its upstream
[end-of-life](https://devguide.python.org/versions/){target=_blank}.
Support for a Python version may be removed in a minor release after the
version has reached its end-of-life. Older releases remain installable on
those Python versions.

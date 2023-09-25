## Bugs

* The [`ZeroMQConnector`][proxystore_ex.connectors.dim.zmq.ZeroMQConnector] will raise a `_pickle.UnpicklingError: pickle data was truncated` error if the serialized data is larger than the chunk size (64 MiB by default).
    * Affected versions: `==0.4.1`
    * Fixed in `0.5.0` by [PR #219](https://github.com/proxystore/proxystore/pull/219){target=_blank}.

## Compatibility

* [ProxyStore Endpoints](guides/endpoints.md) are not supported for
  Python 3.7 on ARM-based Macs because
  [aiortc](https://aiortc.readthedocs.io){target=_blank} does not have the corresponding
  wheels. The base ProxyStore package can still be installed on this
  software/hardware configurations---just not with the `endpoints` extras.
    * Affected versions: `>=0.4.0,<0.6.0`
    * Python 3.7 support will be removed in `0.6.0`. See [Issue #236](https://github.com/proxystore/proxystore/issues/236){target=_blank}.
* Newer versions of [UCX-Py](https://github.com/rapidsai/ucx-py){target=_blank}
  no longer support Python 3.7.
    * We test against and recommend using UCX-Py `0.30.00`.
    * Affected versions: `>=0.4.0,<0.6.0`
    * Python 3.7 support will be removed in `0.6.0`. See [Issue #236](https://github.com/proxystore/proxystore/issues/236){target=_blank}.

## MyPy

### Implicit re-exports

*Affected versions:* `<=0.5.1`

Examples throughout the documentation generally suggest importing
[`Store`][proxystore.store.base.Store] like the following.
```python title="example.py"
from proxystore.store import Store
```

MyPy will raise an attribute defined error if run with `--no-implicit-reexport`.
```bash
$ mypy example.py --no-implicit-reexport
example.py:1: error: Module "proxystore.store" does not explicitly export attribute "Store"  [attr-defined]
```

This can be fixed by using the explicit import.
```python
from proxystore.store.base import Store
```

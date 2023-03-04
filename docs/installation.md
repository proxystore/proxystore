## Base Install

We always recommend installing packages inside of your virtual environment of choice.
E.g.,
```bash
$ python -m venv venv
$ . venv/bin/activate
```

Once your virtual environment is activated, install ProxyStore with `pip`.
```bash
$ pip install proxystore
```

## Extras Options

The base installation is designed to be as lightweight as possible, but
ProxyStore provides many features with extra dependencies that can be installed with the appropriate extras option.

| Install | Purpose |
| :------ | :------ |
| `#!bash pip install proxystore[all]` | Install all extras except `dev` and `docs` |
| `#!bash pip install proxystore[endpoints]` | Use [ProxyStore Endpoints](guides/endpoints.md) |
| `#!bash pip install proxystore[redis]` | Use the [`RedisConnector`][proxystore.connectors.redis.RedisConnector] |
| `#!bash pip install proxystore[zmq]` | Use the [`ZeroMQConnector`][proxystore.connectors.dim.zmq.ZeroMQConnector] |
| `#!bash pip install proxystore[dev]` | Development dependencies |
| `#!bash pip install proxystore[docs]` | Documentation dependencies |

Multiple extras options can be install at the same time.

```bash
$ pip install proxystore[endpoints,redis]
```

Or everything can be installed at once (this does not install the development packages).

```bash
$ pip install proxystore[all]
```

### Distributed In-Memory Connectors

The [`MargoConnector`][proxystore.connectors.dim.margo.MargoConnector] and
[`UCXConnector`][proxystore.connectors.dim.ucx.UCXConnector] have additional
manual installation steps to be completed before they can be used.


* **Margo:**
    * Install [Mochi-Margo](https://github.com/mochi-hpc/mochi-margo){target=_blank} and the dependencies
    * Install [Py-Mochi-Margo](https://github.com/mochi-hpc/py-mochi-margo){target=_blank}
* **UCX:**
    * Install [UCX](https://github.com/openucx/ucx){target=_blank}
    * Install [UCX-Py](https://github.com/rapidsai/ucx-py){target=_blank}

## ProxyStore Extensions

Additional features are available in the `proxystore-extensions` package
([Repository](https://github.com/proxystore/extensions){target=_blank} and
[Docs](https://extensions.proxystore.dev){target=_blank}).
Features in the extensions package tend to be more experimental or have heavier dependencies.
```bash
$ pip install proxystore-extensions
```

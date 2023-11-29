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
| `#!bash pip install proxystore[extensions]` | Install the [`proxystore-ex`](https://github.com/proxystore/extensions){target=_blank} package |
| `#!bash pip install proxystore[redis]` | Use the [`RedisConnector`][proxystore.connectors.redis.RedisConnector] |
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

## ProxyStore Extensions

Additional features are available in the
[`proxystore-ex`](https://pypi.org/project/proxystore-ex/){target=_blank}
package ([repository](https://github.com/proxystore/extensions){target=_blank}
and [docs](https://extensions.proxystore.dev){target=_blank}).
Features in the extensions package tend to be more experimental or have heavier
not pip-installable dependencies.

The extensions package can be installed alongside ProxyStore.
```bash
$ pip install proxystore[extensions]
```
Or standalone.
```bash
$ pip install proxystore-ex
```

Rather than importing from `proxystore_ex` directly, ProxyStore re-exports
all packages and modules via the [`proxystore.ex`][proxystore.ex] submodule.

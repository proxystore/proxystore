## Base Install

We always recommend installing packages inside of your virtual environment of choice.
E.g.,
```bash
python -m venv venv
. venv/bin/activate
```

Once your virtual environment is activated, install ProxyStore with `pip`.
```bash
pip install proxystore
```

## Extras Options

The base installation is designed to be as lightweight as possible, but
ProxyStore provides many features with extra dependencies that can be installed with the appropriate extras option.

| Install | Purpose |
| :------ | :------ |
| `#!bash pip install proxystore[all]` | Install all extras |
| `#!bash pip install proxystore[endpoints]` | Use [ProxyStore Endpoints](guides/endpoints.md) |
| `#!bash pip install proxystore[kafka]` | Use [Kafka stream shims][proxystore.stream.shims.kafka] |
| `#!bash pip install proxystore[redis]` | Use [Redis stream shims][proxystore.stream.shims.redis] or the [`RedisConnector`][proxystore.connectors.redis.RedisConnector] |
| `#!bash pip install proxystore[zmq]` | Use [ZeroMQ stream shims][proxystore.stream.shims.zmq] |

Multiple extras options can be installed at the same time.

```bash
pip install proxystore[endpoints,redis]
```

Or everything can be installed at once (this does not install the development packages).

```bash
pip install proxystore[all]
```

!!! note

    Some features depend on packages which cannot be installed with `pip`.
    For example, the [`DAOSConnector`][proxystore.connectors.daos.DAOSConnector]
    requires PyDAOS which is installed alongside DAOS.
    See the [DAOS guide](guides/daos.md) for details.

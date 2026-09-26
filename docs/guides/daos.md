# DAOS

This guide shows you how to use ProxyStore with DAOS.

The Distributed Asynchronous Object Storage (DAOS) is a distributed object
store designed for high-speed non-volatile memory storage like Intel Optane
and NVMe. Systems such as ALCF's Aurora provide DAOS deployments.

ProxyStore provides support for DAOS via the
[`DAOSConnector`][proxystore.connectors.daos.DAOSConnector] which uses
[PyDAOS](https://www.intel.com/content/www/us/en/developer/articles/case-study/unlock-the-power-of-daos-in-python-with-pydaos.html){target=_blank}
internally to connect to a DAOS pool.

References:

* [DAOS Documentation](https://docs.daos.io/){target=_blank}
* [PyDAOS Introduction](https://www.intel.com/content/www/us/en/developer/articles/case-study/unlock-the-power-of-daos-in-python-with-pydaos.html){target=_blank}
* [PyDAOS v2.4 Implementation](https://github.com/daos-stack/daos/tree/release/2.4/src/client/pydaos){target=_blank}

## Installation

PyDAOS is installed alongside DAOS and is not available on PyPI, so it is
not included in any of ProxyStore's extras installation options.
Typically, PyDAOS is installed into the system Python's `site-packages`
(e.g., `/usr/lib64/python3.X/site-packages/pydaos/`).

To use PyDAOS with ProxyStore, create a virtual environment with ProxyStore
installed and make the system `pydaos` package importable within that
environment, either by copying the package into the environment's
`site-packages` or by adding its parent directory to your `PYTHONPATH`.

```bash
# Load necessary modules (names vary by system)
module load daos

# Create a virtual environment with ProxyStore installed
python -m venv venv
. venv/bin/activate
pip install proxystore

# Copy the system pydaos into our environment
cp -r /usr/lib64/python3.X/site-packages/pydaos/ \
    $VIRTUAL_ENV/lib/python3.Y/site-packages/

# Verify that pydaos imports
python -c "import pydaos"
```

!!! warning

    PyDAOS includes a compiled extension module, so the copied package must
    have been built for a Python version compatible with the one in your
    virtual environment. If the import fails, contact your system
    administrators about a PyDAOS build for a newer Python version.

## Create a DAOS Pool and Container

PyDAOS requires an existing DAOS pool and container.
DAOS pools are typically allocated by system administrators (at ALCF, for
example, pools can be requested from ALCF support).
Once you have a DAOS pool and its name, you can create a container in the pool.
The type must be `PYTHON` for use with PyDAOS, but the container label
can be anything you want.

```bash
daos container create $POOL_NAME --type=PYTHON --label=demo-container
```

## Create a Connector

Creating a [`DAOSConnector`][proxystore.connectors.daos.DAOSConnector]
is simple.

```python
from proxystore.connectors.daos import DAOSConnector

with DAOSConnector(
    pool=...,
    container='demo-container',
    namespace='proxystore',
) as connector:
    key = connector.put(b'data')
    assert connector.exists(key)
    assert connector.get(key) == b'data'

    connector.evict(key)
    assert not connector.exists(key)
```

The `namespace` argument is used as the name for the DAOS dictionary created
within the DAOS container that you provided. All operations by the connector
will be done within that "namespace" or dictionary. This is helpful for
preventing ProxyStore from clashing with other operations from other programs
on the same container.

## Using with a Store

A [`DAOSConnector`][proxystore.connectors.daos.DAOSConnector] can be used
to initialize a ProxyStore [`Store`][proxystore.store.base.Store].
Learn more about the [`Store`][proxystore.store.base.Store] interface in the
[Get Started](../get-started.md) guide.

```python
from proxystore.connectors.daos import DAOSConnector
from proxystore.store import Store

connector = DAOSConnector(
    pool=...,
    container='demo-container',
    namespace='proxystore',
)

with Store('my-store', connector) as store:
    key = store.put(my_object)
    assert store.get(key) == my_object

    p = store.proxy(my_object)
    assert isinstance(p, type(my_object))
```

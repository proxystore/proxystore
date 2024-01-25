# ProxyStore

![PyPI - Version](https://img.shields.io/pypi/v/proxystore)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/proxystore)
![GitHub License](https://img.shields.io/github/license/proxystore/proxystore)

[![docs](https://github.com/proxystore/proxystore/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/proxystore/actions/workflows/docs.yml)
[![tests](https://github.com/proxystore/proxystore/actions/workflows/tests.yml/badge.svg?label=tests)](https://github.com/proxystore/proxystore/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/proxystore/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/proxystore/main)

ProxyStore is a library that facilitates efficient data management in distributed Python applications.
At the core of ProxyStore is the [*proxy*](https://docs.proxystore.dev/main/concepts/proxy/) object which acts as a transparent reference to an object living in a global object store.
This pass-by-reference interface with just-in-time object resolution works across processes, machines, and sites, and enables data producers to change the low-level communication method dynamically without altering application code or behavior.

ProxyStore accelerates the development of dynamic task-based workflows, serverless applications, and more by (1) decoupling control flow from data flow, (2) abstracting low-level communication mechanisms, and (3) eliminating the need for shims, wrapper functions, and boilerplate code.

ProxyStore supports a diverse set of programming paradigms:

* [Task-based Workflows](https://arxiv.org/abs/2303.08803)
* [Function-as-a-Service/Serverless Applications](https://docs.proxystore.dev/main/guides/globus-compute/)
* [Distributed Futures](https://docs.proxystore.dev/main/guides/proxy-futures/)
* [Bulk Data Streaming](https://docs.proxystore.dev/main/guides/streaming/)
* and more!

ProxyStore provides support for many third-party mediated communication methods
out-of-the-box including
[DAOS](https://docs.daos.io/v2.4/),
[Globus Transfer](https://www.globus.org/data-transfer),
[Kafka](https://kafka.apache.org/),
[KeyDB](https://docs.keydb.dev/), and
[Redis](https://redis.io/).
Custom communication methods built on
[Mochi](https://mochi.readthedocs.io/en/latest/margo.html),
[UCX](https://openucx.org/),
[WebRTC](https://webrtc.org/), and
[ZeroMQ](https://zeromq.org/)
are provided for high-performance and peer-to-peer applications.

Read more about ProxyStore's concepts [here](https://docs.proxystore.dev/main/concepts/).
Complete documentation for ProxyStore is available at
[docs.proxystore.dev](https://docs.proxystore.dev).

## Installation

The base ProxyStore package can be installed with [`pip`](https://pip.pypa.io/en/stable/).
```bash
pip install proxystore
```

Many features require dependencies that are not installed by default but can
be enabled via extras installation options such as `endpoints`, `kafka`, or `redis`.
*All* optional dependencies can be installed with:
```bash
pip install proxystore[all]
```
This will also install the [`proxystore-ex`](https://extensions.proxystore.dev/)
package which contains extension and experimental features.
The extensions package can also be installed with `pip` using
`proxystore[extensions]` or `proxystore-ex`.

See the [Installation](https://docs.proxystore.dev/main/installation) guide
for more information about the available extras installation options.
See the [Contributing](https://docs.proxystore.dev/main/contributing) guide
to get started for local development.

## Example

Getting started with ProxyStore requires a few lines of code.

```python
from proxystore.connectors.redis import RedisConnector
from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store

store = Store('my-store', RedisConnector('localhost', 6379))

# Store the object and get a proxy. The proxy acts
# like a reference to the object.
data = MyDataType(...)
proxy = store.proxy(data)
assert isinstance(proxy, Proxy)

def my_function(x: MyDataType) -> ...:
    # x is resolved my-store on first use transparently to the
    # function. Then x behaves as an instance of MyDataType.
    assert isinstance(x, MyDataType)

my_function(proxy)  # Succeeds
```

Check out the [Get Started](https://docs.proxystore.dev/main/get-started)
guide to learn more!

## Citation

[![DOI](https://zenodo.org/badge/357984234.svg)](https://zenodo.org/badge/latestdoi/357984234)

If you use ProxyStore or any of this code in your work, please cite our [SC '23 paper](https://dl.acm.org/doi/10.1145/3581784.3607047).
```bibtex
@inproceedings{pauloski2023proxystore,
    author = {Pauloski, J. Gregory and Hayot-Sasson, Valerie and Ward, Logan and Hudson, Nathaniel and Sabino, Charlie and Baughman, Matt and Chard, Kyle and Foster, Ian},
    title = {{Accelerating Communications in Federated Applications with Transparent Object Proxies}},
    address = {New York, NY, USA},
    articleno = {59},
    booktitle = {Proceedings of the International Conference for High Performance Computing, Networking, Storage and Analysis},
    doi = {10.1145/3581784.3607047},
    isbn = {9798400701092},
    location = {Denver, CO, USA},
    numpages = {15},
    publisher = {Association for Computing Machinery},
    series = {SC '23},
    url = {https://doi.org/10.1145/3581784.3607047},
    year = {2023}
}
```

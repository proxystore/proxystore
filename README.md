# ProxyStore

![PyPI - Version](https://img.shields.io/pypi/v/proxystore)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/proxystore)
![GitHub License](https://img.shields.io/github/license/proxystore/proxystore)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8077899.svg)](https://doi.org/10.5281/zenodo.8077899)

[![docs](https://github.com/proxystore/proxystore/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/proxystore/actions/workflows/docs.yml)
[![tests](https://github.com/proxystore/proxystore/actions/workflows/tests.yml/badge.svg?label=tests)](https://github.com/proxystore/proxystore/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/proxystore/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/proxystore/main)

ProxyStore facilitates efficient data flow management in distributed Python applications, such as dynamic task-based workflows or serverless and edge applications.

The [*transparent object proxy*](https://docs.proxystore.dev/latest/concepts/proxy/), the core building block within ProxyStore, acts like a wide-area reference that can be cheaply communicated.
Unlike traditional references that are only valid within the virtual address space of a single process, the proxy references an object in remote storage and can be implicitly dereferenced in arbitrary processes—even on remote machines.
The proxy is transparent in that it implicitly dereferences its target object when used—referred to a *just-in-time resolution*—and afterwards forwards all operations on itself to the cached target object.

This paradigm results in the best of both pass-by-reference and pass-by-value semantics, improves performance and portability by reducing transfer overheads through intermediaries, and abstracts low-level communication methods which reduces code complexity.
A proxy contains within itself all the information and logic necessary to resolve the target object.
This self-contained nature means a proxy consumer need not be aware of the low-level communication mechanisms used by the proxy; rather, this is unilaterally determined by the producer of the proxy.

ProxyStore supports a diverse set of programming patterns built on the proxy paradigm:

* [Task-based Workflows](https://arxiv.org/abs/2303.08803)
* [Function-as-a-Service/Serverless Applications](https://docs.proxystore.dev/latest/guides/globus-compute/)
* [Distributed Futures](https://docs.proxystore.dev/latest/guides/proxy-futures/)
* [Bulk Data Streaming](https://docs.proxystore.dev/latest/guides/streaming/)
* and more!

ProxyStore can leverage many popular mediated data transfer and storage systems:
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

Read more about ProxyStore's concepts [here](https://docs.proxystore.dev/latest/concepts/).
Complete documentation for ProxyStore is available at
[docs.proxystore.dev](https://docs.proxystore.dev).

## Installation

The base ProxyStore package can be installed with [`pip`](https://pip.pypa.io/en/stable/).
```bash
pip install proxystore
```

Leveraging third-party libraries may require dependencies not installed by default but can be enabled via extras installation options (e.g., `endpoints`, `kafka`, or `redis`).
*All* additional dependencies can be installed with:
```bash
pip install proxystore[all]
```
This will also install the [`proxystore-ex`](https://extensions.proxystore.dev/)
package which contains extensions and experimental features.
The extensions package can also be installed with `pip` using
`proxystore[extensions]` or `proxystore-ex`.

See the [Installation](https://docs.proxystore.dev/latest/installation) guide
for more information about the available extras installation options.
See the [Contributing](https://docs.proxystore.dev/latest/contributing) guide
to get started for local development.

## Example

Using ProxyStore to store and transfer objects only requires a few lines of code.

```python
from proxystore.connectors.redis import RedisConnector
from proxystore.proxy import Proxy
from proxystore.store import Store

data = MyDataType(...)

def my_function(x: MyDataType) -> ...:
    # x is transparently resolved when first used by the function.
    # Then the proxy, x, behaves as an instance of MyDataType
    # for the rest of its existence.
    assert isinstance(x, MyDataType)

with Store(
    'example',
    connector=RedisConnector('localhost', 6379),
    register=True,
) as store:
    # Store the object in Redis (or any other connector).
    # The returned Proxy acts like a reference to the object.
    proxy = store.proxy(data)
    assert isinstance(proxy, Proxy)

    # Invoking a function with proxy works without function changes.
    my_function(proxy)
```

Check out the [Get Started](https://docs.proxystore.dev/latest/get-started)
guide to learn more!

## Citation

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8077899.svg)](https://doi.org/10.5281/zenodo.8077899)

If you use ProxyStore or any of this code in your work, please cite our ProxyStore ([SC '23](https://dl.acm.org/doi/10.1145/3581784.3607047)) and Proxy Patterns ([TPDS](https://ieeexplore.ieee.org/document/10776778)) papers.
```bib
@inproceedings{pauloski2023proxystore,
    title = {Accelerating {C}ommunications in {F}ederated {A}pplications with {T}ransparent {O}bject {P}roxies},
    author = {Pauloski, J. Gregory and Hayot-Sasson, Valerie and Ward, Logan and Hudson, Nathaniel and Sabino, Charlie and Baughman, Matt and Chard, Kyle and Foster, Ian},
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

@article{pauloski2024proxystore,
    title = {Object {P}roxy {P}atterns for {A}ccelerating {D}istributed {A}pplications},
    author = {Pauloski, J. Gregory and Hayot-Sasson, Valerie and Ward, Logan and Brace, Alexander and Bauer, André and Chard, Kyle and Foster, Ian},
    doi = {10.1109/TPDS.2024.3511347},
    journal = {IEEE Transactions on Parallel and Distributed Systems},
    number = {},
    pages = {1-13},
    volume = {},
    year = {2024}
}
```

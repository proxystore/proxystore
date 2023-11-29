# ProxyStore

[![docs](https://github.com/proxystore/proxystore/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/proxystore/actions/workflows/docs.yml)
[![tests](https://github.com/proxystore/proxystore/actions/workflows/tests.yml/badge.svg?label=tests)](https://github.com/proxystore/proxystore/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/proxystore/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/proxystore/main)

ProxyStore provides pass-by-reference semantics for distributed Python
applications through transparent object proxies. Moving data via proxies
(1) decouples control flow from data flow, (2) enables producers to
unilaterally (i.e., without the agreement of or awareness by the consumer)
choose the best storage and communication channel for the data, and (3)
perform just-in-time data movement directly between producer and consumer.

ProxyStore's goals are to:

* **Improve productivity.** ProxyStore enables easy decoupling of
  communication from the rest of the code, allowing developers to focus
  on functionality and performance.
* **Improve compatibility.** Consumers of data can be agnostic to the
  communication method because object proxies handle the communication
  behind the scenes.
* **Improve performance.** Transport methods and object stores can be changed
  at runtime to optimal choices for the given data without the consumers
  being aware of the change.

ProxyStore provides support for many third-party mediated communication methods
out-of-the-box including
[Globus Transfer](https://www.globus.org/data-transfer),
[KeyDB](https://docs.keydb.dev/), and
[Redis](https://redis.io/).
Custom communication methods built on
[Mochi](https://mochi.readthedocs.io/en/latest/margo.html),
[UCX](https://openucx.org/),
[WebRTC](https://webrtc.org/), and
[ZeroMQ](https://zeromq.org/)
are provided for high-performance and multi-site applications.

Read more about ProxyStore's concepts [here](https://docs.proxystore.dev/main/concepts/).
Complete documentation for ProxyStore is available at
[docs.proxystore.dev](https://docs.proxystore.dev).

## Installation

The base ProxyStore package can be installed with [`pip`](https://pip.pypa.io/en/stable/).
```bash
$ pip install proxystore
```

Many features require dependencies that are not installed by default but can
be enabled via extras installation options.
Install *all* optional dependencies with:
```bash
$ pip install proxystore[all]
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

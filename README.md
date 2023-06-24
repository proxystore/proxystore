# ProxyStore

[![DOI](https://zenodo.org/badge/357984234.svg)](https://zenodo.org/badge/latestdoi/357984234)
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

```bash
$ pip install proxystore
$ pip install proxystore[all]
```

See the [Installation](https://docs.proxystore.dev/main/installation) guide for more information about the available extra install options.
For local development, see the [Contributing](https://docs.proxystore.dev/main/contributing) guide.

Additional features are available in the [`proxystore-extensions`](https://github.com/proxystore/extensions) package.

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

If you use ProxyStore or any of this code in your work, please cite the following [paper](https://arxiv.org/abs/2305.09593).
```
@misc{pauloski2023proxystore,
    author = {J. Gregory Pauloski and Valerie Hayot-Sasson and Logan Ward and Nathaniel Hudson and Charlie Sabino and Matt Baughman and Kyle Chard and Ian Foster},
    title = {{Accelerating Communications in Federated Applications with Transparent Object Proxies}},
    archiveprefix = {arXiv},
    eprint = {2305.09593},
    primaryclass = {cs.DC},
    year = {2023}
}
```

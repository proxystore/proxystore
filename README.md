# ProxyStore

![PyPI - Version](https://img.shields.io/pypi/v/proxystore?cache-control=no-cache)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/proxystore?cache-control=no-cache)
![GitHub License](https://img.shields.io/github/license/proxystore/proxystore?cache-control=no-cache)

[![docs](https://github.com/proxystore/proxystore/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/proxystore/actions/workflows/docs.yml?cache-control=no-cache)
[![tests](https://github.com/proxystore/proxystore/actions/workflows/tests.yml/badge.svg?label=tests)](https://github.com/proxystore/proxystore/actions?cache-control=no-cache)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/proxystore/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/proxystore/main?cache-control=no-cache)

ProxyStore provides pass-by-reference semantics for distributed Python applications via [*transparent object proxies*](https://docs.proxystore.dev/latest/concepts/proxy/).

A proxy is a lightweight reference to an object in remote storage that can be cheaply sent to any process, even on a remote machine.
The proxy resolves its target object just-in-time when first used and then behaves like the target object, so code consuming a proxy needs no changes and no knowledge of how the data is stored or moved.
This reduces transfer overheads through intermediaries, such as workflow schedulers or cloud services, and decouples application logic from communication code.

ProxyStore is used to build:

* Task-based workflows (e.g., [Dask Distributed](https://docs.proxystore.dev/latest/guides/dask-distributed/))
* Serverless applications (e.g., [Globus Compute](https://docs.proxystore.dev/latest/guides/globus-compute/))
* [Distributed futures](https://docs.proxystore.dev/latest/guides/proxy-futures/)
* [Bulk data streaming](https://docs.proxystore.dev/latest/guides/streaming/)

Objects can be stored in and transferred via shared file systems, Redis, Globus Transfer, or [ProxyStore Endpoints](https://docs.proxystore.dev/latest/guides/endpoints/) for peer-to-peer transfer.
See the [Connectors](https://docs.proxystore.dev/latest/api/connectors/) reference for all options or to implement your own.

Learn more in the [Concepts](https://docs.proxystore.dev/latest/concepts/) overview and the complete documentation at [docs.proxystore.dev](https://docs.proxystore.dev).

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

See the [Installation](https://docs.proxystore.dev/latest/installation) guide
for more information about the available extras installation options.
See the [Contributing](https://docs.proxystore.dev/latest/contributing) guide
to get started for local development.

## Example

Proxies are cheap to send to other processes and resolve themselves when used.

```python
from concurrent.futures import ProcessPoolExecutor

from proxystore.connectors.file import FileConnector
from proxystore.store import Store


def process(data: dict[str, str]) -> str:
    # The proxy resolves itself to the dict when first used and
    # then behaves exactly like the dict.
    return data['hello']


if __name__ == '__main__':
    with Store('example', FileConnector('./proxystore-data')) as store:
        # Put the object in the store and get back a proxy, a lightweight
        # reference which is cheap to send to other processes.
        proxy = store.proxy({'hello': 'world'})

        # Functions can be invoked with the proxy without any changes.
        with ProcessPoolExecutor() as pool:
            assert pool.submit(process, proxy).result() == 'world'
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

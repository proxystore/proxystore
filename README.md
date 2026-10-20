# ProxyStore

![PyPI - Version](https://img.shields.io/pypi/v/proxystore?cache-control=no-cache)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/proxystore?cache-control=no-cache)
![GitHub License](https://img.shields.io/github/license/proxystore/proxystore?cache-control=no-cache)

[![docs](https://github.com/proxystore/proxystore/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/proxystore/actions/workflows/docs.yml?cache-control=no-cache)
[![tests](https://github.com/proxystore/proxystore/actions/workflows/tests.yml/badge.svg?label=tests)](https://github.com/proxystore/proxystore/actions?cache-control=no-cache)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/proxystore/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/proxystore/main?cache-control=no-cache)

ProxyStore moves large data between Python processes and machines without sending it through your task scheduler, workflow system, or serverless platform.

Instead of passing an object, you pass a [proxy](https://docs.proxystore.dev/latest/concepts/proxy/): a small reference that is cheap to send anywhere.
The proxy fetches the real object the first time your code uses it and then behaves just like that object, so task code doesn't need to change.

## Is ProxyStore for You?

ProxyStore helps if:

* Your workflow spans sites, such as HPC clusters, cloud VMs, and your laptop, and data needs to move with the tasks.
* You run Python tasks across processes or machines using a framework like [Dask](https://docs.proxystore.dev/latest/guides/dask-distributed/), [Globus Compute](https://docs.proxystore.dev/latest/guides/globus-compute/), [Parsl](https://docs.proxystore.dev/latest/examples/), or [`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html).
* Task inputs or outputs are large, and passing them through the framework is slow, uses too much memory, or hits size limits.
* You want to choose how data moves (shared file system, Redis, ZeroMQ, Globus Transfer, or [ProxyStore Endpoints](https://docs.proxystore.dev/latest/guides/endpoints/)) without rewriting application code.

You probably don't need ProxyStore if your objects are small, everything runs in one process, or you need a database with queries and long-term storage.

## Examples

### Local Processes

Only the small proxy goes through the process pool.
The data goes through the store (here, a shared directory) and is fetched by the worker when it is used.

```python
from concurrent.futures import ProcessPoolExecutor

from proxystore.connectors.file import FileConnector
from proxystore.store import Store


def total(data: bytes) -> int:
    # The proxy fetches the bytes when first used and then
    # behaves exactly like the bytes object.
    return len(data)


if __name__ == '__main__':
    data = bytes(100_000_000)  # 100 MB

    with Store(FileConnector('./proxystore-data')) as store:
        # Put the object in the store and get back a proxy.
        proxy = store.proxy(data)

        # Without ProxyStore, all 100 MB would be pickled and sent through
        # the pool. With a proxy, only a small reference is sent.
        with ProcessPoolExecutor() as pool:
            assert pool.submit(total, proxy).result() == 100_000_000
```

To proxy task results too, wrap the executor in a [`StoreExecutor`](https://docs.proxystore.dev/latest/api/store/executor/).
It proxies the arguments and results you choose and evicts their data from the store once they are no longer needed.

```python
from concurrent.futures import ProcessPoolExecutor

from proxystore.connectors.file import FileConnector
from proxystore.store import Store
from proxystore.store.executor import ProxyType
from proxystore.store.executor import StoreExecutor


def double(data: bytes) -> bytes:
    return data + data


if __name__ == '__main__':
    store = Store(FileConnector('./proxystore-data'))

    # Proxy every bytes argument and result. Closing the executor
    # also closes the store.
    with StoreExecutor(
        ProcessPoolExecutor(),
        store,
        should_proxy=ProxyType(bytes),
    ) as executor:
        result = executor.submit(double, bytes(50_000_000)).result()
        assert len(result) == 100_000_000
```

### Across Sites with Endpoints

HPC compute nodes and cloud VMs are often behind firewalls that block incoming connections, which makes moving data between sites hard.
[ProxyStore Endpoints](https://docs.proxystore.dev/latest/guides/endpoints/) are object stores that you run at each site and that send data directly to each other, peer-to-peer, with no open ports or SSH tunnels.
If a direct connection isn't possible, endpoints fall back to encrypted relays (public or self-hosted).
Endpoints only talk to peers you allow, and a proxy created at one site can be used at any other site with a peered endpoint.

Set up an endpoint on each system (e.g., your laptop and an HPC cluster), and add the other system's endpoint as a peer.
```bash
pip install proxystore[endpoints]
proxystore-endpoint configure my-endpoint  # Prints this endpoint's ID.
proxystore-endpoint peers add my-endpoint other-site OTHER_ENDPOINT_ID
proxystore-endpoint start my-endpoint
```

Then, use the `EndpointConnector` in place of the `FileConnector`.
Here, the task runs on the cluster via [Globus Compute](https://docs.proxystore.dev/latest/guides/globus-compute/), and the cluster's endpoint fetches the data directly from your laptop's endpoint.
The connector takes the IDs of both endpoints, and the proxy uses whichever one is on the system where it is resolved.
Endpoints limit the size of each object (100 MB by default), which you can change in the [endpoint configuration](https://docs.proxystore.dev/latest/guides/endpoints/#endpoint-cli).
```python
from globus_compute_sdk import Executor

from proxystore.connectors.endpoint import EndpointConnector
from proxystore.store import Store


def total(data: bytes) -> int:
    return len(data)


connector = EndpointConnector(
    ['<Laptop Endpoint ID>', '<Cluster Endpoint ID>']
)

with Store(connector) as store:
    proxy = store.proxy(bytes(50_000_000))  # 50 MB

    with Executor(endpoint_id='<Globus Compute Endpoint UUID>') as gce:
        assert gce.submit(total, proxy).result() == 50_000_000
```

Check out the [Get Started](https://docs.proxystore.dev/latest/get-started) guide to learn more!

## Installation

Install ProxyStore with [`pip`](https://pip.pypa.io/en/stable/).
```bash
pip install proxystore
```

Some connectors and features need extra dependencies, which you can install with extras options (e.g., `endpoints`, `kafka`, or `redis`).
All extra dependencies can be installed with:
```bash
pip install proxystore[all]
```

See the [Installation](https://docs.proxystore.dev/latest/installation) guide for the full list of extras options.

## Learn More

* [Documentation](https://docs.proxystore.dev): the full docs, including the API reference and FAQ.
* [Concepts](https://docs.proxystore.dev/latest/concepts/): how proxies, stores, and connectors fit together, and how to build on them if you're writing your own framework.
* [Guides](https://docs.proxystore.dev/latest/guides/): using ProxyStore with Dask and Globus Compute, [distributed futures](https://docs.proxystore.dev/latest/guides/proxy-futures/), [data streaming](https://docs.proxystore.dev/latest/guides/streaming/), and more.
* [Connectors](https://docs.proxystore.dev/latest/api/connectors/): all the ways to store and move data, or how to write your own.
* [Contributing](https://docs.proxystore.dev/latest/contributing): set up a local development environment.

## Citation

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8077899.svg)](https://doi.org/10.5281/zenodo.8077899)

If you use ProxyStore or any of this code in your work, please cite our ProxyStore ([SC '23](https://dl.acm.org/doi/10.1145/3581784.3607047)) and Proxy Patterns ([TPDS](https://ieeexplore.ieee.org/document/10776778)) papers.

<details markdown="1">
<summary>BibTeX</summary>

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

</details>

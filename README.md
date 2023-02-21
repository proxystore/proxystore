# ProxyStore

[![docs](https://github.com/proxystore/proxystore/actions/workflows/docs.yml/badge.svg)](https://github.com/proxystore/proxystore/actions/workflows/docs.yml)
[![tests](https://github.com/proxystore/proxystore/actions/workflows/tests.yml/badge.svg?label=tests)](https://github.com/proxystore/proxystore/actions)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/proxystore/proxystore/main.svg)](https://results.pre-commit.ci/latest/github/proxystore/proxystore/main)

ProxyStore provides a unique interface to object stores through transparent
object proxies that is designed to simplify the use of object stores for
transferring large objects in distributed applications.
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

## Installation

Install via pip:
```bash
# Base install
pip install proxystore
# Extras install for serving Endpoints
pip install proxystore[endpoints]
```

More details are available on the [Get Started](https://docs.proxystore.dev/getstarted) guide.
For local development, see the [Contributing](https://docs.proxystore.dev/contributing) guide.

## Documentation

Complete documentation for ProxyStore at https://proxystore.dev.

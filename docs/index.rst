.. ProxyStore documentation master file, created by
   sphinx-quickstart on Tue Apr 20 23:25:40 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to ProxyStore's documentation!
======================================

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

ProxyStore can be found on `GitHub <https://github.com/proxystore/proxystore>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   getstarted
   advanced
   guides
   contributing
   api
   changelog

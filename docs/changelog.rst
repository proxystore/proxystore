Changelog
#########

Version 0.2.0
-------------

`Released 17 May 2021`

**Summary of Changes**

#. Proxies created by :any:`Store.proxy() <proxystore.store.base.Store.proxy>` can auto-evict the data from the store after the proxy has resolved itself using the `evict=True` flag (`#0ef8f61 <https://github.com/gpauloski/ProxyStore/commit/0ef8f617118926737c85936adf2c0355150d93ee>`_).
#. Added cloudpickle to ProxyStore serialization utilities for lambda pickling support (`#a992ec7 <https://github.com/gpauloski/ProxyStore/commit/a992ec756b40551fa36455e39d4bc617cb7cc2ce>`_).
#. Added :any:`LambdaFactory <proxystore.factory.LambdaFactory>` (`#fc7674a <https://github.com/gpauloski/ProxyStore/commit/fc76746a432cfe6f50214bece98ebe956abd848b>`_).
#. Extensive ProxyStore refactor to separate proxy/factory building blocks from the key-value store implementations. See `Issue #8 <https://github.com/gpauloski/ProxyStore/issues/8>`_ and `Pull Request #9 <https://github.com/gpauloski/ProxyStore/pull/9>`_ for more details (`#0564e5f <https://github.com/gpauloski/ProxyStore/commit/0564e5f437cc34097528dd93256460a4bf1e6345>`_).
#. Pass custom factories to :any:`Store.proxy() <proxystore.store.base.Store.proxy>` (`#dffba4c <https://github.com/gpauloski/ProxyStore/commit/dffba4c7b0a81ea12f91d75c1ab014ded435868b>`_).
#. Initialize multiple factories of the same type (`#1411c0f <https://github.com/gpauloski/ProxyStore/commit/1411c0f638e22cdb4ea0047fa97137c84eab8538>`_ and `#41b4bd3 <https://github.com/gpauloski/ProxyStore/commit/41b4bd3c4e432ac00c3b9c3c91fb911fb1450353>`_).


Version 0.1.1
-------------

`Released 28 April 2021`

**Summary of Changes**

#. Added ProxyStore integration into example FuncX and Parsl applications (`#eaa1782 <https://github.com/gpauloski/ProxyStore/commit/eaa1782dedb2436ecbee0d9ea4e11c932720b12a>`_).
#. Added support for preserialized objects (`#c2c2283 <https://github.com/gpauloski/ProxyStore/commit/c2c228316cdfbbd31a3642839bc9b4e9884c2be7>`_).
#. Changed :any:`Proxy <proxystore.proxy.Proxy>` to inherit from the pure Python slots Proxy from `lazy-object-proxy <https://github.com/ionelmc/python-lazy-object-proxy>`_ rather than the C version. Closes `#1 <https://github.com/gpauloski/ProxyStore/issues/1>`_ (`#5c77eb0 <https://github.com/gpauloski/ProxyStore/commit/5c77eb08f6128344aba53f200dad30ddcf035daf>`_).
#. Extensive docstring and documentation updates.
#. Minor unittest updates.

Version 0.1.0
-------------

`Released 21 April 2021`

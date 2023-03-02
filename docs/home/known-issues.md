* [ProxyStore Endpoints](../guides/endpoints.md) are not supported for
  Python 3.7 on ARM-based Macs because
  [aiortc](https://aiortc.readthedocs.io){target=_blank} does not have the corresponding
  wheels. The base ProxyStore package can still be installed on this
  software/hardware configurations---just not with the `endpoints` extras.
* Newer versions of [UCX-Py](https://github.com/rapidsai/ucx-py){target=_blank}
  no longer support Python 3.7.

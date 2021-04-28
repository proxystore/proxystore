from proxystore.proxy import to_proxy
from proxystore.backend.utils import init_local_backend
from proxystore.backend.utils import init_redis_backend

import proxystore.factory as factory
import proxystore.proxy as proxy
import proxystore.utils as utils

global store
store = None

__version__ = '0.1.1'

import lazy_object_proxy

from proxystore.factory import Factory


class Proxy(lazy_object_proxy.Proxy):
    """Lazy Object Proxy

	An object proxy acts as a thin wrapper around a Python object, i.e.
	the proxy behaves identically to the underlying object. The proxy is
	initialized with a callable factory object. The factory returns the
	underlying object when called, i.e. 'resolves' the proxy. The proxy
	does not call the factory until the first access to the proxy (hence, the
	lazy aspect of the proxy).
    """
    def __init__(self, factory: Factory) -> None:
        """Create a proxy object

        Args:
            factory (Factory): callable factory object that returns the
				underlying object when called. The factory is only called
				once unless the proxy is manually reset.
        """
        super(Proxy, self).__init__(factory)

    def __reduce__(self):
        """Helper method for pickling

        Override `Proxy.__reduce__` so that we only pickle the Factory
        and not the object itself to reduce size of the pickle.
        """
        return Proxy, (self.__factory__,)

    def __reduce_ex__(self, protocol):
        """See `__reduce__`"""
        return self.__reduce__()

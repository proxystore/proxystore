from proxystore.connectors.dim.zmqstream import ZeroMQConnector 
from proxystore.connectors.dim.zmqstream import ProxyStream
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import Store

store = Store('stream', ZeroMQConnector(address='localhost', port=5555, timeout=10))
register_store(store)

store = get_store('stream')  

packet = 'DATA'
stream = store.put(packet)
stream = store.put(packet + '2')


assert isinstance(stream, type(ProxyStream))  
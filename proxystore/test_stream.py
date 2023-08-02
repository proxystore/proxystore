from proxystore.connectors.dim.margo import MargoConnector 
from proxystore.store import get_store
from proxystore.store import register_store
from proxystore.store import Store

store = Store('stream', MargoConnector(protocol="ofi+tcp", port=5555, timeout=10))
register_store(store)

store = get_store('stream')  

stream = store.create_stream()
packet = 'DATA'
store.put(packet, key=stream)
store.put(packet + '2', key=stream)


print(store.get(stream))
print(store.get(stream))
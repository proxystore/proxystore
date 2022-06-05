Peer-to-Peer Endpoints
######################

ProxyStore :any:`Endpoints  <proxystore.endpoint>` are in-memory object stores
with peering capabilities. Endpoints enable data transfer with proxies
between multiple sites using NAT traversal.

.. warning::

   Endpoints are experimental and the interfaces and underlying
   implementations will likely change. Refer to the API docs for the most
   up-to-date information.

.. warning::

   Endpoints do not have user authentication yet, so use at your own risk.

Overview
--------

At its core, the :any:`Endpoint <proxystore.endpoint.endpoint.Endpoint>` is
an in-memory data store built on asyncio. Endpoints provide a REST
API, served using `Quart <https://pgjones.gitlab.io/quart/>`_, and ProxyStore
provides the :any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` as
the primary interface for clients to interact with endpoints.

.. figure:: ../static/endpoints.png
   :align: center
   :figwidth: 100 %
   :alt: ProxyStore Endpoints

   **Figure 1:** ProxyStore Endpoints overview. Clients can make requests to any endpoint
   and those request will be forwarded to the correct endpoint. Endpoints
   establish peer-to-peer connections using UDP hole-punching and a publicly
   accessible signaling server.

Unlike popular in-memory data stores (Redis, Memcached, etc.), ProxyStore
endpoints can operate as peers even from behind different NATs without the need
to open ports or SSH tunnels. To achieve direct data transfer between peers,
endpoints use the `WebRTC <https://webrtc.org/>`_ standard to determine
how the peers can connect.

As shown in **Fig. 1**, endpoints use a commonly accessible *signaling server*
to facilitate peer connections. When an endpoint is started, the Endpoint
registers with the signaling server. Then, when an endpoint needs to make a
request from a peer, (1) the endpoint creates an *offer* and asks the
signaling server to forward the offer to the peer endpoint. The signaling
server forwards the offer (2) and the peer endpoint creates an *answer* to the
received offer. The peer endpoint returns the *answer* to the original
endpoint via the signaling server (3, 4).

The offer and answer contain information about the local and remote sessions
of the endpoints which can be used to complete the peer-to-peer connection (5).
(*Note*: this is a great simplification and more details can be found at
`<https://webrtc.org/getting-started/peer-connections>`_.) The peers will then
keep a data channel open between themselves for the remainder of their
lifetime.

Clients interacting with an endpoint via the REST API and typical object store
operations (*get*, *set*, etc.) specify a *key* and an *endpoint UUID*.
Endpoints that receive a request with a different endpoint UUID will attempt
a peer connection to the endpoint if one does not exist already and forward
the request along and facilitate returning the response back to the client.

Endpoint CLI
------------

Endpoints can be configure and started with the ``proxystore-endpoint``
command.

.. code-block:: bash

   $ proxystore-endpoint configure my-endpoint --port 9732 --server remote-server.com:3574
   Configured endpoint my-endpoint <12b8f3b6-6c0e-4141-b851-870895e3eb3c>.

   To start the endpoint:
       $ proxystore-endpoint start my-endpoint.

Endpoint configurations are stored to ``~/.proxystore/{endpoint-name}`` and
contain the name, UUID, host address, port, and singaling server address.

#. **Name:** readable name of the endpoint. Used for management in the CLI and
   to improve log readability.
#. **UUID:** the primary identifier of the endpoint. The signaling server will
   use this UUID to keep track of endpoints.
#. **Host address:** the address of the host that this endpoint will be running
   on. If unspecified when configuring the endpoint, the IP address of the
   current host will be used.
#. **Port:** port the endpoint REST server will be listening on (defaults to
   9753).
#. **Signaling server address**: address of signaling server to use for peer
   connections. All endpoints that may peer with each other must use the same
   signaling server. Signaling servers are optional, and if unspecified, the
   endpoint will operate without peering functionalities.

Starting the endpoint will load the configuration from the ProxyStore home
directory, initialize the endpoint, and start a Quart app using the host and
port.

.. code-block:: bash

   $ proxystore-endpoint start my-endpoint

EndpointStore
-------------

The primary interface to endpoints is the
:any:`EndpointStore <proxystore.store.endpoint.EndpointStore>`.

.. note::

   This section assumes familiarity with proxies and the
   :any:`Store <proxystore.store.base.Store>` interface. See the
   :ref:`quick-start` guide before getting started with endpoints.

.. code-block:: python

   import proxystore as ps

   store = ps.store.init_store(
       'endpoint',
       name='default',
       endpoints=[
           '5349ffce-edeb-4a8b-94a6-ab16ade1c1a1',
           'd62910f6-0d29-452e-80b7-e0cd601949db',
           ...
       ],
    )

    p = store.proxy(my_object)

The :any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` takes
a list of endpoint UUIDs. This list represents any endpoint that proxies
created by this store may interact with to resolve themselves. The
:any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` will use this
list to find its *home* endpoint, the endpoint that will be used to issue
operations to. To find the *home* endpoint, the ProxyStore home directory
(``~/.proxystore``) will be scanned for any endpoint configurations matching
the one of the UUIDs. If a match is found, the
:any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` will attempt
to connect to the endpoint using the host and port in the configuration. This
process is repeated until a reachable endpoint is found. While the user could
specify the home endpoint directly, the home endpoint may change when a proxy
travels to a different machine.

Proxy Lifecycle
---------------

In distributed systems, proxies created from an
:any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` can be used
to facilitate simple and fast data communication.

#. Host A creates a proxy of the *target* object. The serialized *target*
   is placed in Host A's home endpoint. The proxy contains the key referencing
   the *target*, the endpoint UUID with the *target* data, and the list of
   all endpoint UUIDs configured with the
   :any:`EndpointStore <proxystore.store.endpoint.EndpointStore>`.
#. Host A ships the proxy off.
#. Host B receives the proxy and attempts to use the proxy initiating the
   proxy *resolve* process.
#. The proxy looks for an initialized
   :any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` and
   initializes one if necessary. This process will find Host B's home endpoint.
#. The proxy requests the *target* from the local
   :any:`EndpointStore <proxystore.store.endpoint.EndpointStore>` using
   the key and endpoint UUID the data is stored on.
#. Host B's home endpoint receives the request for the data, sees it is on
   a remote endpoint, and initiates a peer connection to Host A's endpoint.
#. After the peer connection is establish, Host A's home endpoint sends the
   data to Host B's home endpoint.
#. Host B's home endpoint returns the data to Host B and the proxy is
   resolved.

Hosting a Signaling Server
--------------------------

Currently, ProxyStore does not provided any publicly host signaling servers,
though we hope to in the future! Hosting your own signaling server is simple
if you have a host accessible from the internet (e.g., a compute instance from
a cloud provider or a machine behind a NAT with an open port) and the
ProxyStore package installed.

.. code-block:: bash

   $ signaling-server --port 3579

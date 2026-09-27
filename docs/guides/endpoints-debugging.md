# Endpoints Debugging

*Last updated 26 September 2026*

This guide outlines some common trouble-shooting steps to take if you
are encountering issues using ProxyStore Endpoints.

## Test a Local Endpoint

Consider you configured and started an endpoint as follows:
```bash
$ proxystore-endpoint configure myendpoint
INFO: Configured endpoint: myendpoint <f4dc841d-377e-4785-8d66-8eade34f63cd>
INFO: Config and log file directory: ~/.local/share/proxystore/myendpoint
INFO: Start the endpoint with:
INFO:   $ proxystore-endpoint start myendpoint
$ proxystore-endpoint start myendpoint
INFO: Starting endpoint process as daemon.
INFO: Logs will be written to ~/.local/share/proxystore/myendpoint/log.txt
```

### Check Endpoint Logs
Endpoint logs are written to a directory in `$XDG_DATA_HOME/proxystore` which
in this case is `~/.local/share/proxystore/myendpoint`
(see [`home_dir()`][proxystore.utils.environment.home_dir] for the full
specification).
```bash
$ grep "Serving endpoint" ~/.local/share/proxystore/myendpoint/log.txt
INFO  (proxystore.endpoint.serve) :: Serving endpoint f4dc841d-377e-4785-8d66-8eade34f63cd (myendpoint) on 127.0.1.1:8766
```
The logs are the first place to check for any potential issues.

If you see an error similar to:
```
[Errno 8] nodename nor servname provided, or not known
```
Try changing `host = "fqdn"` to `host = "ip"` in the `config.toml` file in the endpoint directory.

### Monitor the Endpoint
Debug level logging can be enabled when starting the endpoint, and
the endpoint can be run directly in the terminal instead of as a daemon process
via the `--no-detach` flag. These two options are helpful for live monitoring
the endpoint.
```bash
$ proxystore-endpoint --log-level DEBUG start myendpoint --no-detach
```

### Use the Test CLI
The `proxystore-endpoint` CLI provides a `test` subcommand for testing endpoint commands.
See the [CLI Reference](../api/cli.md#proxystore-endpoint-test){target=_blank}.
```bash
$ proxystore-endpoint test myendpoint exists abcdef
INFO: Object exists: False
```
As expected, an object with key `abcdef` does not exist in the store, but
we got a valid response so we know the endpoint is running correctly.
You can also validate that this request was logged by the endpoint.

### Connect from Python
The [`EndpointClient`][proxystore.endpoint.client.EndpointClient] can be used
to connect to an endpoint directly. Clients find the endpoint's address,
token, and TLS certificate fingerprint (if enabled) in the `connection.json`
file that the endpoint writes to its directory when it starts, and
[`EndpointClient.from_name()`][proxystore.endpoint.client.EndpointClient.from_name]
reads this file for you.
```python
from proxystore.endpoint.client import EndpointClient

with EndpointClient.from_name('myendpoint') as client:
    print(client.info)
    print(client.exists('abcdef'))
```

### Common Errors

* **An endpoint named ... does not exist**: No endpoint with that name is
  configured in the ProxyStore home directory. Check the name with
  `proxystore-endpoint list` and that the client uses the same ProxyStore home
  directory as the endpoint.
* **Unable to find the connection file of the endpoint**: The endpoint is
  not running, or the client cannot read the endpoint directory. Clients on
  other nodes need the ProxyStore home directory on a shared file system.
  If the error says the endpoint process is running, the endpoint was likely
  started with an older version of ProxyStore. Restart it with
  `proxystore-endpoint stop NAME` and `proxystore-endpoint start NAME`.
* **The endpoint failed to prove that it knows the endpoint token**: The
  endpoint was restarted while the client was connecting, or a different
  process is listening on the endpoint's address (e.g., after the endpoint
  stopped). Restart the endpoint and try again.
* **The endpoint responded with HTTP**: The endpoint is running an older
  version of ProxyStore. Restart the endpoint with the same version as the
  client.
* **Endpoint returned HTTP error code 426**: The client is using an older
  version of ProxyStore than the endpoint. Upgrade ProxyStore on the client.
* **`EndpointVersionWarning`**: The client and endpoint use different
  ProxyStore versions or Python minor versions. See
  [Version Compatibility](endpoints.md#version-compatibility).

## Test a Remote Endpoint

Consider I have an endpoint named "myendpoint" running on system A with ID
`aaaa7ce803e5348b74920943c61322d9b38fcddf2decd628c9abc3c224610929` and another named "otherendpoint" on system B with ID
`bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e`.

### Check the Peers
Each endpoint must have the other in its peers. On system A, check that
system B's endpoint is listed, and vice versa on system B.
```bash
$ proxystore-endpoint peers list myendpoint
NAME     ID
=========================================================================
system-b bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e
```
If an endpoint is missing, add it with
[`proxystore-endpoint peers add`](../api/cli.md#proxystore-endpoint-peers-add).
Also check that peering is enabled (`enabled = true` in the `[p2p]` section
of the endpoint's `config.toml`).

### Check the Endpoint Logs
When peering is enabled, the endpoint logs the addresses it listens on for
peer connections and whether it connected to its home relay.
```
INFO  (proxystore.p2p.manager) :: PeerManager[self(aaaa7ce803)]: listening for peer connections on 0.0.0.0:41421, [::]:36871
INFO  (proxystore.p2p.manager) :: PeerManager[self(aaaa7ce803)]: connected to home relay
```
If the endpoint does not connect to a home relay, it logs a warning. The
endpoint can still connect directly to peers, but peers behind NATs may not
be able to reach it. Check that the relays are reachable from the system or
configure self-hosted relays (see
[Relays](endpoints.md#relays)).

The endpoint also logs the network path of each peer connection when it is
established and when the path changes. A connection often starts relayed
and becomes direct once hole-punching succeeds.
```
INFO  (proxystore.p2p.manager) :: PeerManager[self(aaaa7ce803)]: connection to peer system-b(bbbb75951c) is relayed via https://usw1-1.relay.n0.iroh.link./ (rtt 16 ms)
INFO  (proxystore.p2p.manager) :: PeerManager[self(aaaa7ce803)]: connection to peer system-b(bbbb75951c) is direct to 203.0.113.7:57600 (rtt 12 ms)
```

### Ping a Peer
The `proxystore-endpoint test ... ping` command measures the latency between
two endpoints and reports whether their connection is direct or relayed.
```bash
$ proxystore-endpoint test --remote bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e myendpoint ping
INFO: Reply from bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e: time=218.10 ms path=direct to 203.0.113.7:57600 (rtt 12 ms)
INFO: Reply from bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e: time=13.31 ms path=direct to 203.0.113.7:57600 (rtt 12 ms)
INFO: Reply from bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e: time=13.09 ms path=direct to 203.0.113.7:57600 (rtt 12 ms)
INFO: Reply from bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e: time=13.25 ms path=direct to 203.0.113.7:57600 (rtt 12 ms)
INFO: 4 ping(s): min/avg/max = 13.09/64.44/218.10 ms
```
The time is measured by the local endpoint, and the first ping includes the
time to connect to the peer. If the connection stays relayed, transfers
between the endpoints will be slower. This typically happens when both
endpoints are behind NATs which prevent hole-punching or a firewall blocks
UDP traffic. Without `--remote`, the command measures the latency between
the client and the local endpoint.

### Use the Test CLI
The `proxystore-endpoint test` CLI can be used to establish a peer connection
between two endpoints and invoke remote operations.
Here, we will request the endpoint on system A to invoke an `exists`
operation on the endpoint on system B.
```bash
$ proxystore-endpoint test --remote bbbb75951c623dbfd969e4ec8c7406e00bb8603814ef6db50c1f9780bc60714e myendpoint exists abcdef
INFO: Object exists: False
```

You will get an error if the peer request fails. Check the logs of both
endpoints for further error messages. Common errors are:

* **Endpoint ... is not in the allowlist of peers**: The local endpoint does
  not have the remote endpoint in its peers.
* **Peer ... refused the connection because this endpoint is not in its
  allowlist of peers**: The remote endpoint does not have the local endpoint
  in its peers. The remote endpoint logs a warning that it refused the
  connection.
* **Failed to connect to peer ...** or **Connecting to peer ... timed out**:
  The remote endpoint is not running, or the endpoints cannot reach each
  other. If relays are disabled (`relays = "none"`), the endpoints can only
  connect directly. If discovery is unavailable, an endpoint can only reach
  peers whose addresses are cached in its `peer-addrs.json` file.
* **Peer ...: Data size ... exceeds the maximum object size**: The object is
  larger than the `max_object_size` of the remote endpoint.

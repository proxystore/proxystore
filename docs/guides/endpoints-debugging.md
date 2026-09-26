# Endpoints Debugging

*Last updated 25 September 2026*

This guide outlines some common trouble-shooting steps to take if you
are encountering issues using ProxyStore Endpoints.

## Test a Local Endpoint

Consider you configured and started an endpoint as follows:
```bash
$ proxystore-endpoint configure myendpoint
INFO: Configured endpoint myendpoint <f4dc841d-377e-4785-8d66-8eade34f63cd>. Start with:
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
Try changing the `host_type` parameters from `fqdn` to `ip` in the `config.toml` file in the endpoint directory.

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
to connect to an endpoint directly. Clients authenticate with the token (and
TLS certificate, if enabled) that the endpoint writes to its directory when it
starts, and
[`connect_to_endpoint()`][proxystore.endpoint.client.connect_to_endpoint]
reads these files for you.
```python
import os

from proxystore.endpoint.client import connect_to_endpoint
from proxystore.endpoint.config import read_config

endpoint_dir = os.path.expanduser('~/.local/share/proxystore/myendpoint')
config = read_config(endpoint_dir)
with connect_to_endpoint(config, endpoint_dir) as client:
    print(client.info)
    print(client.exists('abcdef'))
```

### Common Errors

* **Unable to find the token or certificate file**: The endpoint is not
  running, or the client cannot read the endpoint directory. Clients on
  other nodes need the ProxyStore home directory on a shared file system.
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

Consider I have an endpoint running on system A with UUID
`aaaa0259-5a8c-454b-b17d-61f010d874d4` and another on System B
with UUID `bbbbab4d-c73a-44ee-a316-58ec8857e83a`.

### Check Relay Server Connections
Both endpoints must be connected to the same relay server to form a peer
connection. First, check the `address` value in the `[relay]` section
is present and set to the correct URI string.
The endpoint config is found in the `config.toml` file in the endpoint
directory (e.g., `~/.local/share/proxystore/myendpoint/config.toml`).
Restart your endpoints if you had to change the configuration.

Second, confirm the endpoint connects to the relay server when started by
checking the endpoint logs for a line like this.
```bash
INFO  (proxystore.p2p.relay_client) :: Established client connection to relay server at ws://localhost:8765 with client uuid=aaaa0259-5a8c-454b-b17d-61f010d874d4 and name=myendpoint
```

### Use the Test CLI
The `proxystore-endpoint test` CLI can be used to establish a peer connection
between two endpoints and invoke remote operations.
Here, we will request the endpoint on system A (named "myendpoint") to invoke
an `exists` operation on the endpoint on system B.
```bash
$ proxystore-endpoint test --remote bbbbab4d-c73a-44ee-a316-58ec8857e83a myendpoint exists abcdef
INFO: Object exists: False
```

You will get an error if the peer connection fails. For example:
```bash
ERROR: Endpoint returned ERROR for EXISTS request: Request to peer bbbbab4d-c73a-44ee-a316-58ec8857e83a failed: ...
```
If this happens, check the logs for both endpoints for further error messages.
Peer requests typically fail for two reasons:

1. One of the endpoints is not running (e.g., an endpoint crashed) or is not
   connected to the relay server.
2. One of the endpoints is behind a symmetric NAT. The NAT traversal
   techniques used to establish peer-to-peer connections between endpoints
   are not reliable across symmetric NATs or poorly behaved legacy NATs.

### Check Peer-to-Peer Compatibility
After ensuring both endpoints are running and connected to the relay server,
you can check the NAT compatibility in two ways.

1. Endpoints will attempt to discover and log the NAT behavior on startup, so
   check the logs to see if this could be the reason.
   ```
   INFO  (proxystore.p2p.nat) :: Checking NAT behavior. This may take a moment...
   INFO  (proxystore.p2p.nat) :: NAT Behavior:   Endpoint-independent mapping
   INFO  (proxystore.p2p.nat) :: External IP:    <IP ADDRESS>
   INFO  (proxystore.p2p.nat) :: External Port:  <PORT>
   INFO  (proxystore.p2p.nat) :: NAT traversal for peer-to-peer methods (e.g., hole-punching) is likely to work.
   ```
   A NAT with *address-dependent* mapping assigns a different external address
   to each peer, so the address one peer learns is not the address it must
   send to and hole-punching will not work reliably.
2. Use the
   [`proxystore-endpoint check-nat`](../api/cli.md#proxystore-endpoint-check-nat)
   command to discover your NAT behavior.
   ```
   $ proxystore-endpoint check-nat
   INFO: Checking NAT type. This may take a moment...
   INFO: NAT Type:       Full-cone NAT
   INFO: External IP:    <IP ADDRESS>
   INFO: External Port:  <PORT>
   INFO: NAT traversal for peer-to-peer methods (e.g., hole-punching) is likely to work. (NAT traversal does not work reliably across symmetric NATs or poorly behaved legacy NATs.)
   ```

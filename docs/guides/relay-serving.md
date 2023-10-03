# Relay Serving

A relay server facilitates establishing peer to peer connections between
two ProxyStore [`Endpoints`][proxystore.endpoint.endpoint.Endpoint].
Hosting your own relay server is simple if you have a host accessible from the
internet (e.g., a compute instance from a cloud provider or a machine behind a
NAT with an open port) and the ProxyStore package installed.

## Local Serving

The [`proxystore-relay`](../api/cli.md#proxystore-relay) CLI is installed with
the ProxyStore package and is used to serve a relay server instance.

```bash
$ proxystore-relay --port 8700
```

This relay server would be accessible at `ws://localhost:8700`. For example,
an endpoint can be configure with this URI and will connect this instance
when started.

```bash
$ proxystore-endpoint configure my-endpoint --relay-server ws://localhost:8700`
$ proxystore-endpoint start my-endpoint --no-detach
```

Here you would see the endpoint register with the relay server instance. See
the [Endpoints Overview](endpoints.md) for more on how endpoints interact
with a relay server.

### Enabling TLS

In the above example, we connected to the relay with `ws://` which indicates
that the connection is unencrypted. The relay can be served using TLS
encryption if a valid SSL certificate is provided.

!!! alert
    This guide will not describe how to create a valid SSL certificate and
    private key file because the steps can change depending on the
    environment. The rest of the guide assumes `cert.pem` and the
    corresponding `privkey.pem` exist.

Advanced serving, such as TLS encryption, requires a relay configuration file.

```toml title="relay.toml"
port = 8700
certfile = "cert.pem"
keyfile = "privkey.pem"
```

The relay can be started using the `relay.toml` file and will be accessible
at `wss://localhost:8700` (note the change in protocol from `ws://` to
`wss://`).

```bash
$ proxystore-relay --config relay.toml
```

### Logging Behavior

The relay logs to `stdout` at the `INFO` level and above by default. This
behavior can be changed via the `--log-level` and `--log-dir` CLI
options or via the configuration file.

!!! note
    CLI options can be combined with a configuration file, and CLI options
    will override the values in the configuration file if both are provided.

The logging configuration is set in the `[logging]` section. All
configurations are optional with defaults defined in
[`RelayLoggingConfig`][proxystore.p2p.relay.config.RelayLoggingConfig].

```toml title="relay.toml"
[logging]
log_dir = "/path/to/log/dir"
default_log_level = "INFO"
websockets_log_level = "WARNING"
current_client_interval = 60
current_client_limit = 32
```

## User Authentication

A relay provides no user authentication by default. This means that any client
can connect to any other client as long as they know the clients UUID. This
may be suitable for internal or development purposes, but users should take
extra precautions to ensure sensitive data is not exposed.

The relay implementation supports serving with
[Globus Auth](https://www.globus.org/globus-auth-service){target=_blank}.
The following describes the steps required to create a Globus developer
application and serve the relay with Globus Auth.

!!! note
    The following guide is based on
    [Action Provider Tools](https://action-provider-tools.readthedocs.io/en/latest/setting_up_auth.html){target=_blank}.

### Register an Application

Reference: https://docs.globus.org/api/auth/developer-guide/#register-app

1. Visit the
   [Globus Developer Dashboard](https://app.globus.org/settings/developers/){target=_blank}
   and sign in.
2. Select the option to "Register a portal, science gateway, or other
   application you host."
3. Create a new project or register the application under an existing project
   if you have one.

    * The "App Name" is the name displayed on the Globus login and user
      consent pages when users request access tokens.
    * The redirect can be the standard Globus Auth callback:
      `https://auth.globus.org/v2/web/auth-code`.
    * The remaining options can be left to the default or adjusted to your
      needs.

4. Register the application and navigate to the application dashboard under the
   project your application was registered to.
5. Record the client UUID and create a new client secret. Save the client
   secret because it will not be accessible if you lose it.

### Configure Scopes

Reference: https://docs.globus.org/api/auth/reference/#create_scope

Here we will add a new scope to our application. This is necessary because
clients of our relay will need to request this scope for the relay server
to authenticate the clients.

!!! note
    Some of these steps use the `jq` command which is not installed by default
    on most machines but is very helpful for formatting the JSON responses
    in a readable format.

1. Export your client UUID and secret.
   ```bash
   $ export PROXYSTORE_GLOBUS_CLIENT_ID=...
   $ export PROXYSTORE_GLOBUS_CLIENT_SECRET=...
   ```
2. Inspect current scopes of the application.
   ```bash
   $ curl -s --user $PROXYSTORE_GLOBUS_CLIENT_ID:$PROXYSTORE_GLOBUS_CLIENT_SECRET https://auth.globus.org/v2/api/clients/$PROXYSTORE_GLOBUS_CLIENT_ID | jq
   ```
   You should see the `scopes` field is empty.
3. Create a file containing our scopes document `scope.json`.
   ```json
   {
       "scope": {
           "name": "Register with the ProxyStore Relay Server",
           "description": "Register with the ProxyStore Relay Server which enables peer connection with other ProxyStore Endpoints owned by you.",
           "scope_suffix": "relay_all",
           "dependent_scopes": [],
           "advertised": true,
           "allow_refresh_tokens": true
       }
   }
   ```
   The fields can be adjusted as necessary, but we suggest keeping
   `allow_refresh_tokens` as `true`.
4. Post the scopes document to the application.
   ```bash
   $ curl -s --user $PROXYSTORE_GLOBUS_CLIENT_ID:$PROXYSTORE_GLOBUS_CLIENT_SECRET -H 'Content-Type: application/json' -XPOST https://auth.globus.org/v2/api/clients/$PROXYSTORE_GLOBUS_CLIENT_ID/scopes -d @scope.json | jq
   ```
5. Confirm our new scope is present in the application.
   ```bash
   $ curl -s --user $PROXYSTORE_GLOBUS_CLIENT_ID:$PROXYSTORE_GLOBUS_CLIENT_SECRET https://auth.globus.org/v2/api/clients/$PROXYSTORE_GLOBUS_CLIENT_ID | jq
   ```
   You will see the scopes UUID in the `scopes` field.
6. Check the scope's details using the UUID found above. (Replace
   `<SCOPE_UUID>` with the actual UUID.)
   ```bash
   $ curl -s --user $PROXYSTORE_GLOBUS_CLIENT_ID:$PROXYSTORE_GLOBUS_CLIENT_SECRET https://auth.globus.org/v2/api/scopes/<SCOPE_UUID> | jq
   ```

### Update the Relay Config

The `[auth]` section of the relay configuration is used to enable
the authentication method of the relay server. Add the following and update
the `client_id` and `client_secret` with the client UUID and secret from the
application registration. The `audience` parameter should also be set to the
client UUID.

```toml title="relay.toml"
[auth]
method = "globus"

[auth.kwargs]
client_id = "..."
client_secret = "..."
audience = "..."
```

The relay server will use the
[Globus token introspection API](https://docs.globus.org/api/auth/reference/#token-introspect){target=_blank}
to authenticate users using the bearer tokens provided in the opening
websocket handshake. The token introspection process will return information
about the user that the token represents, including the intended audiences for
the token. The relay will ensure that it is an intended audience of the token
by matching against the `audience` field provided in the config.

### Run the Relay

After updating the configuration file, the relay can be run normally.

!!! warning
    A relay server should **always** be served with TLS encryption when
    using Globus Auth for user authentication.

```bash
$ proxystore-relay --config relay.toml
```

### Connecting as a Client

The [`RelayClient`][proxystore.p2p.relay.client.RelayClient] can be used to
connect to the server but requires some extra configuration to connect to
the relay that is being served with Globus Auth.

```python
import asyncio

from proxystore.globus.manager import NativeAppAuthManager
from proxystore.p2p.relay.client import RelayClient

RELAY_APP_UUID = '...'
RELAY_APP_SCOPE = 'relay_all'

async def main() -> None:
    manager = NativeAppAuthManager(
        resource_server_scopes={RELAY_APP_UUID: [RELAY_APP_SCOPE]},
    )
    manager.login()
    authorizer = manager.get_authorizer(RELAY_APP_UUID)

    async with RelayClient(
        'wss://localhost:8700',
        # This Authorization header is used by the relay server to authenticate
        # the new user connection
        extra_headers={'Authorization': authorizer.get_authorization_header()},
        # This is only necessary if using a self-signed SSL certificate.
        verify_certificate=False,
    ) as client:
        input('Continue and disconnect?')


if __name__ == '__main__':
    asyncio.run(main())
```

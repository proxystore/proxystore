# MapReduce with Globus Compute and ProxyStore

> Globus Compute was formerly called funcX.
> Learn about upgrading from funcX to Globus Compute
> [here](https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html).

Example of integrating ProxyStore into a Globus Compute app.

```
$ python mapreduce_globus_compute.py -e $ENDPOINT -n 10 -s 100 --ps-redis --ps-redis-port $PORT
$ ENDPOINT={endpoint} bash run.sh
```

The example app supports three ProxyStore backends. If no option is specified,
ProxyStore is not used.

- **File**: `--ps-file --ps-file-dir /tmp/proxystore-dump`
- **Globus**: `--ps-globus --ps-globus-config globus-endpoint-config.json`
- **Redis**: `--ps-redis --ps-redis-port 1234`

See the [API docs](https://docs.proxystore.dev/main/api/connectors/)
for more info on any of the specific backend types.

To learn more about using Globus Compute, checkout the
[Quickstart](https://globus-compute.readthedocs.io/en/latest/quickstart.html).

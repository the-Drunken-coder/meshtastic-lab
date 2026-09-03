# Gateway spike

The one-node spike is the native integration test at
`backend/tests/integration/test_gateway_spike.py`. It starts the pinned official
firmware image, puts `NodeGateway` between the daemon and an official Python
client, completes configuration and NodeDB synchronization, observes a real
outgoing `SIMULATOR_APP` packet, injects a simulated incoming RF packet, rejects
a second client, and reconnects a new official client without restarting the
daemon.

Run it with:

```sh
make gateway-spike
```

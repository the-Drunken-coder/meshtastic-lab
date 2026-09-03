# Meshtastic Lab

Meshtastic Lab is a firmware-in-the-loop network simulator for controlled Meshtastic experiments. One container runs 2 to 10 isolated native firmware processes, a standard Client API gateway for each node, a deterministic directed-link RF medium, a typed API, and the web experiment console.

The firmware still creates, encrypts, queues, retries, floods, relays, acknowledges, and suppresses packets. Meshtastic Lab only decides which configured receivers hear each emitted RF frame. It answers how the pinned firmware behaves under an explicit connectivity graph and traffic pattern. It does not predict outdoor range or model terrain.

## Start

Requirements are Docker Engine 24 or newer with Compose v2, or current Docker Desktop. The `make dev` port preflight needs Python 3.13; the direct Compose path below does not. Allocate at least 4 CPU cores and 6 GB of memory for a 10-node run.

```sh
make dev
```

Open <http://127.0.0.1:8080>. `make dev` first checks the fixed loopback ports, records the current clean repository commit in the image, then compiles collision-enabled native firmware from the pinned source commit. The initial build can take several minutes; later builds use Docker’s cache.

For direct Compose use, start from a clean checkout and supply its revision explicitly. The image build rejects a missing or malformed revision instead of creating a runtime that cannot start:

```sh
MESHTASTICATOR_COMMIT="$(git rev-parse HEAD)" docker compose up --build
```

The application binds only to loopback:

| Purpose | Address |
| --- | --- |
| Web UI, REST API, OpenAPI | `127.0.0.1:8080`, `/docs` for OpenAPI |
| Virtual node 1 | `127.0.0.1:45001` |
| Virtual nodes 2 through 10 | `127.0.0.1:45002` through `127.0.0.1:45010` |

Internal daemon ports `46001` through `46010` are never published.

The host and container port numbers are intentionally one-to-one. A scenario's `apiPort` is the public node port in the container and on the host. Exported scenarios do not carry a separate host-port mapping.

## Connect a normal Meshtastic client

Start the simulation in the UI, wait for `RUNNING`, then point an official client at a node endpoint:

```sh
meshtastic --host 127.0.0.1:45001 --info
meshtastic --host 127.0.0.1:45002 --nodes
```

The Python API works without simulator-specific behavior:

```python
from meshtastic.tcp_interface import TCPInterface

node_1 = TCPInterface(hostname="127.0.0.1", portNumber=45001)
node_1.sendText("hello from an ordinary client", wantAck=False)
```

V1 permits one external client per node. A second client to that same node is rejected without disturbing the first. Different nodes support simultaneous clients. During startup, configuration uses a separate loopback-only control endpoint and public clients are admitted only after the simulation reaches `RUNNING`.

## Scenarios and runs

Scenario fields are editable only while stopped. Use the node-count and RF controls, assign roles, and save. Reset is also stopped-only. It restores the five-node full-mesh scenario and clears archived in-memory daemon logs. It does not delete completed run files. The topology matrix is directed: a row can transmit to a column when its cell is on. Link cells and the Full mesh, Line, Star, and All isolated presets remain available while running, including during traffic runs. Completed exports keep the starting scenario and an ordered timeline of changes made during the run.

The checked-in JSON scenarios under `scenarios/` are valid API payloads. To load one without the UI:

```sh
curl -X PUT http://127.0.0.1:8080/api/scenario \
  -H 'Content-Type: application/json' \
  --data-binary @scenarios/five-node-line.json
```

Use **Export scenario** in the UI or `GET /api/scenario/export` to save the current definition. Completed traffic results are persisted under the Compose data volume at `/data/runs/<run-id>.json` and are available from the UI or `GET /api/traffic/runs/<run-id>/export`. A request may schedule at most 10,000 messages, with at most 600 messages per minute per source and a one-hour duration. The live traffic endpoint returns bounded counters and aggregates. Per-message records appear only in the completed export.

Packet events and traffic-result exports use `schemaVersion: 1`. Packet cursors pair a per-process `streamId` with the event sequence. Reconnecting clients send both values to `/api/events/ws`; the server replays retained events after the cursor. A changed stream clears the old client history before replay, while an expired sequence reports a history gap. The UI refreshes the authoritative lifecycle, scenario, node, and traffic snapshots after any drop notice.

## Metrics

- Generated is application messages created by the offered traffic schedule. Submitted and submission failed come from the firmware admission response for each request, including queue status and pre-transmission rate-limit errors, not from socket writes.
- Delivered counts unique generated messages exposed by an intended receiver. Receiver deliveries and receiver delivery ratio separately count every applicable node reached by each broadcast.
- RF TX counts firmware transmitter events once, even when several receivers hear one frame. RF TX per delivery exposes flooding and retry amplification.
- Relay TX counts transmissions where the transmitting firmware did not originate the packet.
- Airtime uses the actual firmware-produced packet length and the selected modem preset. Receiver count does not multiply it. The total is also broken down by transmitting node.
- ACK success is separate from destination delivery. It applies only to firmware-accepted direct messages because firmware disables acknowledgments for broadcasts. Percentiles remain unavailable until their configured sample minimum is met. Live percentiles use the most recent 2,048 deliveries; completed results use the full run.
- Duplicate and failed or bad receptions come from native firmware local statistics sampled immediately before and after each traffic run. A partial sample keeps the run exportable and sets `failedReceptionMetricsComplete` to false with the missing nodes listed in `missingLocalStatsNodes`. Collision results are labeled `native` only in the collision-enabled image.

See [fidelity](docs/fidelity.md) for exact boundaries.

## Test and acceptance commands

```sh
make test
make lint
make gateway-spike
make integration-test
make acceptance
make browser-smoke
```

`make acceptance` starts the Compose stack when needed, runs a clean three-node relay experiment with three official clients, removes and restores the relay link, runs fixed-rate traffic, validates persisted metrics, stops the simulation, and checks that public node listeners closed.

`make browser-smoke` installs the pinned Playwright Chromium build, starts the real Compose backend, loads the five-node line, exercises lifecycle, link, traffic, and metric controls, then tears the stack down.

## Host support

The supported container paths are Linux `amd64` and Linux `arm64`. Docker Desktop on Apple Silicon builds and runs the native `arm64` target without x86 emulation. Intel macOS uses the `amd64` target. The product is container-only on macOS; a host-native `meshtasticd` build is not the supported V1 path.

## Troubleshooting

- **Start is disabled:** inspect `/api/capabilities`. The image refuses to claim native collision support if its build marker is absent. Rebuild without cache if a partial old image is present.
- **Startup becomes FAILED:** select the named node and `stderr` in Daemon diagnostics. Startup is all-or-nothing and preserves recent child output. Persisted stdout and stderr each retain one rotated 8 MiB backup.
- **Client is rejected:** another external client is already attached to that node. Disconnect it or select another node endpoint.
- **Ports are occupied:** stop another stack or local Meshtastic process using `8080` or `45001` through `45010`. `docker compose down` removes the application container without deleting the result volume.
- **Warm-up reports missing pairs:** local firmware configuration is the readiness gate. NodeInfo exchange is timed and best effort because hop limits, non-relaying roles, and collisions can make graph-connected pairs unobservable. Missing pairs remain visible in the lifecycle message but do not invalidate a correctly configured simulation.
- **Apple Silicon build pressure:** the native firmware builder is large. Increase Docker Desktop’s memory limit if the compiler is killed.

Architecture details are in [architecture](docs/architecture.md), and the gateway contract is in [client connections](docs/client-connections.md).

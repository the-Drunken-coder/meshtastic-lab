# Architecture

Meshtastic Lab uses one deployable container and one native firmware process per virtual radio. The application has no Docker socket, privileged mode, host networking, database, or secondary service.

```text
Browser ── REST / WebSocket ──> FastAPI / SimulatorService
                                      │
Official client :45001 ──> NodeGateway 1 ──> meshtasticd 1 :46001
Official client :45002 ──> NodeGateway 2 ──> meshtasticd 2 :46002
                                      │                │
                                      └── DirectedMedium <── SIMULATOR_APP
                                               │
                                               └── ToRadio injection to receivers
```

Only the gateway ports are published. A daemon has one internal Client API connection owned by its gateway. Publishing the daemon directly would let a second internal controller connection replace the external connection because the upstream native server stores one active API connection. Each gateway also opens an ephemeral loopback-only control endpoint for startup configuration. Public admission stays closed until configuration and warm-up finish, so a reconnecting host client cannot take the startup slot.

## Gateway data flow

`NodeGateway` owns a persistent downstream TCP connection. Its incremental parser accepts split headers, split protobuf bodies, several frames per read, garbage before a valid magic header, and rejects payload lengths above 512 bytes. Ordinary `ToRadio` and `FromRadio` frames retain their original bytes. Parsing is classification and instrumentation, not protocol translation.

External writes and controller writes enter bounded queues and converge on one serialized downstream writer. That prevents frame-byte interleaving. Ordinary downstream frames go to the one active external client through a bounded queue. Outgoing `SIMULATOR_APP` frames instead enter the RF queue and are not exposed as application packets. A disconnected external client can reconnect and repeat the official configuration handshake without restarting the daemon.

## RF transmission flow

1. Native `SimRadio` keeps the firmware-created mesh fields, wraps the RF payload in `Compressed`, and emits `FromRadio.packet` with port `SIMULATOR_APP`.
2. The gateway classifies that frame and places the preserved mesh packet in its bounded RF queue.
3. `DirectedMedium` records one transmitter event and takes one atomic snapshot of the transmitter’s outgoing links.
4. Every enabled receiver gets a copied mesh packet with only the configured RSSI and SNR added. Disabled decisions are recorded but not injected.
5. Receiver firmware unwraps the simulated packet and enters the real radio receive path. It decides decryption, routing, relaying, acknowledgment, retry, duplicate suppression, and client exposure.

Injections to different receivers are concurrent. Medium node loops are independent, so hidden transmitters can inject overlapping frames into one receiver and exercise the native collision window.

## Lifecycle

```text
STOPPED ── start ──> STARTING ── configured ──> WARMING_UP ── timed observation ──> RUNNING
   ▲                       │                         │                               │
   │                       └──────── failure ────────┴──────── failure ─────────> FAILED
   │                                                                                 │
   └──────────────────────────── stop <── STOPPING <──────────── stop ────────────────┘
```

The lifecycle lock serializes commands. Node identity, roles, RF configuration, and channel configuration change only in `STOPPED`. Directed links use a copy-on-write map under an async lock and can change in `RUNNING`, including during traffic. Every run keeps its starting scenario plus the event sequence, monotonic time, and directed link for each in-run change. Start is all-or-nothing. Configuration writes use the official settings transaction, intentionally reboot once, reconnect the gateway, read effective values back, and fail if they differ.

Warm-up sends one best-effort NodeInfo request through each private gateway endpoint, then allows a bounded stabilization interval. Process readiness, gateway readiness, and verified local configuration are hard gates. Network-wide NodeInfo observations are evidence, not a readiness proof. The lifecycle message reports missing graph-connected pairs because hop limits, non-relaying roles, collisions, and transient contention can make them unobservable.

## Processes and concurrency

`NativeProcessSupervisor` assigns each child a unique hardware ID, data directory, internal port, stdout file, stderr file, and state record. It starts children with `asyncio.create_subprocess_exec`. Stream-drain and exit-monitor tasks are bounded by the process record. Tini is container PID 1 and reaps adopted descendants.

The backend event loop owns gateways, medium loops, traffic scheduling, lifecycle state, and WebSocket subscriptions. Blocking official-client configuration calls run in worker threads. Each WebSocket subscriber has a bounded 256-event queue; slow clients receive a dropped-event notice while aggregate metrics remain authoritative. Recent history is a bounded 5,000-event deque.

## Failure handling

A child exit, gateway failure, RF queue overflow, or unexpected medium-worker exit identifies the node and moves the simulator to `FAILED`. Transport overload uses the explicit `SIMULATOR_OVERLOAD` category so it cannot masquerade as mesh congestion. One serialized failure task freezes an active traffic result, archives recent daemon output, stops traffic and medium tasks, closes gateways, and terminates the remaining children. Start and Stop await that task. Graceful child shutdown has an eight-second deadline followed by force-kill. Gateway startup and shutdown also have deadlines. Stop cancels an active traffic run before dismantling the RF path.

Results use a temporary file followed by an atomic rename under `/data/runs`. Terminal results are frozen before that rename, and later firmware packets cannot change them. If persistence fails, the last eight unpersisted terminal results remain available through the API until the backend restarts. The polled live summary contains only bounded counters and aggregate maps; full generated-message records stay in the completed export. No packet or UI queue is unbounded.

## Extension seams

- `backend/app/simulator/medium.py`: a future propagation or replay medium can implement the same firmware RF boundary.
- `backend/app/gateway/node_gateway.py`: future client multiplexing belongs here, not in the daemon process model.
- `backend/app/models/scenario.py`: scenario schema migrations and physical-network extensions.
- `backend/app/traffic/controller.py`: additional offered-load schedules or a later sweep coordinator.
- `backend/app/metrics/`: new fidelity-appropriate counters and exporters.

You are implementing V1 of a standalone, Dockerized Meshtastic network simulator. Use the provisional project name “Meshtastic Lab” unless the repository already has a name.

Do not stop after writing a proposal. Inspect the current upstream projects, document the architecture decision, implement the system, run the tests, and leave the repository in a usable state.

Work autonomously. Make reasonable engineering decisions where details are unspecified and record those decisions in the documentation. Do not ask for approval between implementation stages.

# 1. Product objective

Build a firmware-in-the-loop Meshtastic test environment where:

1. A user starts the system with Docker.
2. The web UI creates and runs a simulated Meshtastic network.
3. The default network contains five virtual Meshtastic radios.
4. The node count is configurable from 2 through 10 before starting the simulation.
5. Every virtual radio exposes its own standard Meshtastic TCP Client API endpoint.
6. Existing Meshtastic-compatible programs can connect to those endpoints without simulator-specific changes.
7. The user can determine, through a directed topology matrix, which radios can hear which other radios.
8. The user can change links while the simulation is running to simulate relay loss or network partitions.
9. The user can generate controlled message traffic and observe delivery, latency, airtime, relaying, retransmissions, collisions, and failures.
10. Meshtastic’s actual native firmware remains responsible for packet creation, encryption, queueing, channel access, acknowledgments, retries, flooding, rebroadcasting, routing, and duplicate suppression.

The simulator must answer:

“Given this explicitly configured link topology and traffic pattern, how does the real Meshtastic firmware behave?”

It must not claim to predict exact real-world radio range.

# 2. Mandatory upstream investigation

Before writing production implementation code:

1. Inspect the current official `meshtastic/Meshtasticator` repository.
2. Inspect the current official `meshtastic/firmware` native Linux or Portduino simulator implementation.
3. Inspect the current official Meshtastic Client API and Python protobuf/library implementation.
4. Run the existing Meshtasticator interactive simulator with at least two nodes.
5. Identify exactly how current native firmware:
   - emits simulated outgoing RF packets;
   - accepts simulated incoming RF packets;
   - represents `SIMULATOR_APP` packets;
   - handles packet timing and collision emulation;
   - exposes its TCP Client API;
   - handles one or more TCP clients;
   - performs its initial `startConfig` exchange.
6. Record:
   - Meshtasticator commit hash;
   - Meshtastic firmware version or image digest;
   - Meshtastic Python package version;
   - protobuf compatibility assumptions;
   - supported container architectures;
   - required collision-emulation build flags.
7. Write these findings to `docs/upstream-investigation.md`.

Do not rely on remembered behavior or old examples.

Do not use an unpinned `latest`, `master`, `develop`, or `beta` dependency in the completed runtime. Pin exact commits, package versions, base-image versions, and the Meshtastic firmware container digest or custom firmware-build commit.

Meshtasticator is attribution-licensed. Preserve required notices and add a `THIRD_PARTY_NOTICES.md` file identifying reused or adapted code and its source revision.

# 3. Non-negotiable implementation rules

## 3.1 Use real firmware

Do not write a simplified Meshtastic routing engine.

Use the native Meshtastic firmware in simulator mode. The simulator backend may decide which receivers hear a transmitted RF frame, but it must not decide application routes, manufacture acknowledgments, retransmit application messages itself, or bypass firmware queues.

## 3.2 Standard client compatibility

Each virtual node must expose a real Meshtastic TCP streaming interface using the official framed `ToRadio` and `FromRadio` protocol.

A normal client must not need a custom SDK or simulator mode.

The following must work through the public virtual-node endpoint:

- complete Client API configuration synchronization;
- read local node information;
- read the node database;
- send a text broadcast;
- send a text direct message;
- receive messages;
- observe acknowledgments and routing errors;
- disconnect and reconnect without restarting the simulation.

Use the official Meshtastic Python library in acceptance tests.

## 3.3 One external client per node in V1

Support exactly one active external client connection per virtual node.

A second client connecting to the same virtual node must be rejected cleanly with a logged reason. It must not corrupt the first client’s stream.

Different virtual nodes must support simultaneous clients.

Multiple external clients attached to one virtual radio are explicitly not part of V1.

## 3.4 Do not assume multiple daemon connections are safe

The simulation controller needs access to the same firmware stream used to observe and inject simulated RF traffic. The external application also needs a complete Client API connection.

Implement a proper per-node gateway or multiplexer. Do not merely publish each internal daemon port and separately connect the simulator backend to it.

Prove the gateway design in a technical spike before building the full UI.

## 3.5 No Docker socket or privileged runtime

The finished application must not mount `/var/run/docker.sock`, run Docker-in-Docker, require privileged mode, or require host networking.

Run the native Meshtastic processes as child processes inside the simulator runtime container, or use another isolated design that does not hand the application control of the host Docker daemon.

# 4. Mandatory gateway spike

Complete this before substantial frontend work.

For one virtual node:

1. Start one native `meshtasticd` process in simulated-radio mode.
2. Give it a unique internal data directory, hardware ID, and internal TCP port.
3. Start a gateway that owns the connection to the daemon.
4. Connect an official Meshtastic client to the gateway’s public TCP port.
5. Complete a normal configuration and NodeDB synchronization.
6. Send a message from the external client.
7. Confirm that the simulator controller can observe the resulting outgoing simulated RF frame.
8. Inject a simulated incoming RF frame through the controller.
9. Confirm that the external client receives the resulting normal Meshtastic packet.
10. Disconnect the external client and successfully reconnect a new client without restarting the daemon.

Write an automated integration test for this spike.

The gateway must correctly handle:

- TCP reads split at arbitrary byte boundaries;
- several framed protobufs arriving in one read;
- partial four-byte headers;
- invalid framing bytes;
- lengths over the protocol maximum;
- external-client disconnects;
- downstream-daemon disconnects;
- backpressure;
- cancellation;
- simultaneous internal and external writes;
- startup timeout;
- shutdown timeout.

Use one serialized downstream write path. Do not permit writes from multiple threads or tasks to interleave frame bytes.

Use bounded queues. Do not create an unbounded packet or event queue.

Whenever possible, preserve original frames byte-for-byte when forwarding them. Parse frames to classify and instrument them, but avoid reserializing unrelated external traffic and thereby discarding unknown future protobuf fields.

Do not proceed on the assumption that the gateway works because a socket opened. The official client must complete its actual configuration handshake.

# 5. Runtime architecture

Use the existing repository conventions if this is not an empty repository. Otherwise use approximately this structure:

meshtastic-lab/
  backend/
    app/
      api/
      gateway/
      simulator/
      traffic/
      metrics/
      models/
      main.py
    tests/
    pyproject.toml
  frontend/
    src/
    package.json
  scenarios/
    five-node-full-mesh.json
    five-node-line.json
    three-node-relay.json
    hidden-terminal.json
  scripts/
    acceptance.py
    smoke-test.sh
  docs/
    upstream-investigation.md
    architecture.md
    fidelity.md
    client-connections.md
  compose.yaml
  Dockerfile
  README.md
  THIRD_PARTY_NOTICES.md

Recommended stack:

- Python with `asyncio` for the simulator, gateway, process management, and API.
- FastAPI or an equivalent typed Python API framework.
- Pydantic or equivalent validated models.
- React and TypeScript for the web UI.
- A lightweight frontend build system.
- Pytest for backend and integration tests.
- Playwright or equivalent for one minimal browser smoke test.
- Current supported language runtimes, pinned through lockfiles and container image digests.

Prefer a single deployable simulator container that:

- launches the Meshtastic child processes;
- runs the gateway listeners;
- runs the simulation medium;
- serves the API;
- serves the compiled frontend.

A separate frontend container is acceptable during development, but `docker compose up --build` must provide one straightforward end-user experience.

Use a small init system or correct PID 1 handling so child processes are reaped and termination signals propagate.

# 6. Ports and process isolation

Use these defaults unless they conflict with the repository:

- Web UI and REST/WebSocket API: `8080`
- Public virtual-node endpoints: `45001` through `45010`
- Internal daemon endpoints: a separate unexposed range such as `46001` through `46010`

Map the public node range through Docker.

Never expose the internal daemon ports to the host.

Every native firmware process must have:

- a unique hardware ID;
- a unique data directory;
- a unique internal Client API port;
- isolated persistent files;
- captured stdout and stderr;
- an explicit process state;
- bounded startup and shutdown handling.

Disable or uniquely configure any secondary native-daemon listeners that would otherwise collide across processes.

Do not use fixed sleeps as the primary readiness mechanism. Wait for actual process and port readiness with an overall deadline.

If one node fails during startup, report which node failed, preserve its logs, stop the other nodes, and leave no orphan processes.

# 7. Simulation lifecycle

Implement an explicit lifecycle state machine:

- `STOPPED`
- `STARTING`
- `WARMING_UP`
- `RUNNING`
- `STOPPING`
- `FAILED`

Rules:

- Node count, node identity, roles, primary channel, and RF settings are editable only while stopped.
- Directed link state may be changed while running.
- Only one load test may run at a time.
- Stopping the simulation cancels the active load test.
- Graceful shutdown has a bounded deadline.
- Processes that do not exit before the deadline are force-killed.
- Starting and stopping the system repeatedly must not leak ports, sockets, tasks, threads, temporary directories, or child processes.
- API commands that are invalid for the current state must return a conflict response with a useful error.
- Concurrent start or stop requests must be serialized and idempotent where reasonable.

Use fresh node state by default for reproducible experiments. Persistent state may be an explicit scenario option, but it must not be the only mode.

# 8. Scenario model

Create a versioned, validated scenario format.

A scenario should contain approximately:

{
  "schemaVersion": 1,
  "name": "five-node-line",
  "seed": 1,
  "nodeCount": 5,
  "rf": {
    "region": "US",
    "modemPreset": "LONG_FAST",
    "frequencySlot": 20,
    "hopLimit": 4,
    "collisionMode": "native"
  },
  "channel": {
    "name": "Simulator",
    "psk": "<test key representation>"
  },
  "nodes": [
    {
      "id": "node-1",
      "displayName": "Node 1",
      "role": "CLIENT",
      "apiPort": 45001
    }
  ],
  "links": [
    {
      "from": "node-1",
      "to": "node-2",
      "enabled": true,
      "rssiDbm": -85,
      "snrDb": 8
    }
  ]
}

Requirements:

- Allow 2–10 nodes.
- Node IDs must be stable and unique.
- Public ports must be unique and inside the allowed range.
- Links are directed.
- `A -> B` does not imply `B -> A`.
- Reject self-links and duplicate directed links.
- Store RSSI and SNR per directed link.
- Disabled links deliver nothing.
- Link changes must take effect atomically for subsequent transmissions.
- Save and load scenarios as JSON.
- Include schema versioning from the beginning.
- Save the exact scenario snapshot with every test result.

For V1, all nodes use one common RF profile and one common primary logical channel.

Do not implement multiple independent physical frequencies or multiple logical Meshtastic channels in V1.

The UI and documentation must distinguish:

- the LoRa RF region, modem preset, and frequency slot;
- the encrypted Meshtastic logical channel.

# 9. Deterministic RF medium

Implement a deterministic directed-link medium.

When firmware node A emits a simulated RF transmission:

1. Record one RF transmission event.
2. Capture the packet identifier, transmitter, packet length, port number, destination, hop fields, timestamp, and any simulator metadata available.
3. Take an atomic snapshot of the current outgoing links from A.
4. For each other node B:
   - if `A -> B` is disabled, record a link-disabled decision;
   - if enabled, inject the frame into B using that link’s configured RSSI and SNR;
   - preserve the actual mesh packet fields produced by the firmware.
5. Do not directly forward the application payload to B’s external client.
6. Let B’s firmware process the frame and decide whether to decode, rebroadcast, acknowledge, discard, or expose it to the client.

Count an RF transmission once per transmitter event, not once per potential receiver.

The same transmitted frame may be heard by several receivers, but that does not mean the transmitter consumed several times the airtime.

Use a monotonic clock for intervals and latency. Store an additional UTC timestamp for display and export.

# 10. Collision behavior

Collision behavior is a V1 requirement because the product is intended to identify congestion and network limits.

Prefer the native Meshtastic simulated-radio collision implementation.

Verify collision support rather than assuming it exists:

1. Create a hidden-terminal topology:
   - node 1 can reach node 2;
   - node 3 can reach node 2;
   - node 1 and node 3 cannot hear one another.
2. Cause node 1 and node 3 to overlap transmissions into node 2.
3. Confirm through packet outcomes or firmware local statistics that the collision produces a bad or dropped reception.
4. Repeat the scenario using a fixed random seed or controlled schedule.
5. Add an automated integration test.

If the official stock image was not built with required collision support, build a custom native firmware image from an exact pinned firmware commit with the required simulator preference enabled.

Do not silently run a no-collision model while labeling results as collision-aware.

Expose collision capability through an API endpoint and in the UI. The result metadata must state the collision implementation used.

If the current upstream collision mechanism is genuinely unusable, isolate an approximate fallback behind a clearly named `CollisionModel` interface, label it `approximate`, document its equations and limitations, and never report it as native firmware behavior. Use that fallback only after making a serious attempt to enable the native mechanism.

# 11. Node configuration

Before exposing a node as ready, configure and verify:

- stable owner long name;
- stable owner short name;
- stable node number or hardware identity;
- common region;
- common modem preset;
- common frequency slot;
- requested hop limit;
- requested role;
- common primary channel.

Do not consider an API write successful merely because the client library returned without an exception. Read the effective configuration back after any reboot or restart and compare it to the requested configuration.

The simulation is ready only after all node configurations have been verified.

Use a bounded warm-up period after configuration so NodeInfo and route discovery traffic can occur. Display whether the simulation is still warming up.

# 12. Web API

Provide a typed REST API and WebSocket event stream.

The exact paths can follow repository conventions, but support these operations:

- health and readiness;
- simulator capabilities;
- retrieve current lifecycle state;
- retrieve and replace the stopped scenario;
- start simulation;
- stop simulation;
- list node states and connection endpoints;
- update one directed link while running;
- apply common topology presets;
- start a traffic run;
- stop a traffic run;
- retrieve current traffic-run state;
- retrieve completed run results;
- retrieve recent packet events;
- export a scenario;
- export a completed run;
- stream live state, node, packet, link, and metric events.

Use generated OpenAPI documentation.

Assign every command and traffic run an ID. Include structured error codes instead of relying only on free-form error text.

WebSocket clients that are slower than the event producer must not cause unbounded memory growth. Use a bounded buffer and report dropped UI events while retaining authoritative aggregate metrics.

# 13. Web UI

Build one functional single-page interface. Do not spend time on elaborate styling.

The page must contain:

## Simulation header

- current lifecycle state;
- current firmware version or digest;
- Start button;
- Stop button;
- Reset button;
- clear startup or failure message.

## RF and channel settings

Editable while stopped:

- node count;
- region;
- modem preset;
- frequency slot;
- hop limit;
- primary logical channel name;
- node roles.

Disable these fields while running and explain why.

## Node list

For every node show:

- name;
- role;
- firmware-process state;
- gateway state;
- external client connected or disconnected;
- public endpoint such as `127.0.0.1:45001`;
- copy-endpoint button;
- recent transmit and receive counts;
- current channel utilization or local-stat value when available.

## Topology matrix

Show a directed matrix where rows are transmitters and columns are receivers.

Each non-diagonal cell must allow enabling or disabling the directed link.

Provide buttons for:

- Full mesh;
- Line;
- Star;
- All isolated.

Provide an optional “edit symmetrically” control, but keep asymmetric links supported.

Runtime link changes must be visibly acknowledged and appear in the event log.

Do not build a geographic map in V1.

## Traffic panel

Allow the user to configure and start one traffic run:

- broadcast text or direct text;
- source node set;
- direct-message destination strategy:
  - fixed;
  - round-robin;
  - deterministic random;
- messages per minute per source;
- payload byte size;
- duration;
- acknowledgment requested;
- random seed.

Allow stopping the run.

## Metrics panel

Show at least:

- generated application messages;
- unique application messages delivered;
- delivery ratio;
- direct-message acknowledgment success ratio;
- median latency;
- p95 latency;
- p99 latency;
- RF transmissions;
- RF transmissions per delivered application message;
- relay transmissions;
- duplicate receptions;
- failed or bad receptions;
- drops by reason;
- observed airtime;
- per-node transmit counts;
- current simulator real-time factor or event-loop lag.

Do not display a percentile when too few samples exist; show it as unavailable rather than zero.

## Packet event table

Show recent events with:

- timestamp;
- event type;
- transmitting node;
- intended destination;
- receiving node or receiver set;
- mesh packet ID;
- application traffic-run sequence when present;
- hop fields;
- RSSI;
- SNR;
- packet type or port number;
- result or drop reason.

Allow filtering by node and event type.

# 14. Traffic generator

The built-in traffic generator must inject messages through the node firmware’s Client API path.

Do not inject traffic directly into the simulated RF medium.

For every generated message:

1. Assign a traffic-run ID and sequence number.
2. Include a compact identifier in the payload.
3. Record the monotonic generation time.
4. submit it to the source node through the same firmware path used by an external application;
5. observe the resulting firmware RF transmissions;
6. detect receipt at the intended application destination;
7. correlate acknowledgment or routing-error packets when applicable.

Support:

- broadcast text;
- direct text;
- payload sizes within the actual protocol limit;
- fixed-rate generation;
- deterministic random selection;
- graceful cancellation.

Validate payload size after encoding the identifier, not before.

The requested rate is an offered application rate. Do not pretend every requested message was accepted if the firmware queue rejected, delayed, or failed it. Track:

- requested;
- submitted;
- submission failed;
- transmitted;
- delivered.

For direct messages, delivery means the intended destination firmware exposed the matching application packet.

For broadcasts, calculate:

- unique generated broadcasts;
- number of unique receiving nodes per broadcast;
- receiver-delivery ratio against the applicable other nodes.

Record acknowledgments separately from destination delivery.

Do not implement an automated multi-rate sweep in V1. Design the result model so a sweep can be added later.

# 15. Airtime and metrics correctness

Use the actual packet length and selected modem parameters to obtain airtime.

Prefer airtime reported or calculated by the current native simulation code when trustworthy. If calculating LoRa time-on-air independently:

- isolate the calculation in one tested module;
- cite the equation in `docs/fidelity.md`;
- add table-driven tests for all supported presets;
- compare several results against the upstream implementation.

Do not multiply transmitter airtime by the number of receivers.

Clearly distinguish:

- application goodput;
- raw or encoded packet bytes;
- RF transmission count;
- receiver delivery count;
- channel utilization;
- per-node transmit airtime;
- duplicate receptions.

Every completed run must save:

- scenario snapshot;
- firmware commit or image digest;
- Meshtasticator source commit;
- client-library version;
- start and finish timestamps;
- random seed;
- collision model;
- generated-message records;
- aggregate metrics;
- relevant failure counters.

Persist completed runs as JSON files under a mounted data directory. Do not introduce PostgreSQL or another database for V1.

Provide JSON export. CSV export for the flat message/event records is desirable but secondary.

# 16. Logging and diagnostics

Use structured backend logs.

Include:

- simulation ID;
- node ID;
- process ID;
- traffic-run ID;
- packet ID where known;
- lifecycle transition;
- link decision;
- error category.

Capture each daemon’s output separately.

The UI must provide access to at least the recent daemon log lines for each node.

Do not expose channel keys or other secrets in ordinary logs.

On startup failure, preserve diagnostics long enough for the user to inspect them.

# 17. Test requirements

## Unit tests

Cover at least:

- framing parser with fragmented input;
- several frames in one read;
- corrupted header recovery;
- excessive frame length;
- downstream write serialization;
- second-client rejection;
- link validation;
- asymmetric-link behavior;
- topology preset generation;
- atomic link updates;
- scenario schema validation;
- deterministic traffic scheduling;
- traffic correlation;
- delivery-ratio calculations;
- broadcast receiver accounting;
- percentile calculations;
- RF-transmission amplification;
- airtime counting once per transmitter;
- lifecycle-state rejection;
- bounded event-buffer behavior.

## Integration tests

Use actual native Meshtastic firmware for these:

1. One official client completes configuration through one gateway.
2. Client disconnect and reconnect.
3. Two clients connect simultaneously to two different virtual nodes.
4. A second client to the same virtual node is rejected.
5. Two fully connected nodes exchange a broadcast.
6. Two fully connected nodes exchange a direct message and acknowledgment.
7. Three-node line:
   - node 1 reaches node 2;
   - node 2 reaches node 3;
   - node 1 cannot directly reach node 3;
   - a node-1-to-node-3 message is delivered through firmware relaying.
8. Removing the node-2-to-node-3 link prevents the previous delivery.
9. Restoring the link permits delivery again without restarting all nodes.
10. Hidden-terminal collision test.
11. Start and stop the simulation three times without leaked processes or occupied ports.
12. Kill one firmware child process and verify the simulator reports the failed node instead of hanging.
13. Stop during an active load run and verify bounded cleanup.
14. Five-node traffic-run smoke test with nonzero messages and metrics.

## Browser smoke test

Automate at least:

1. Open the UI.
2. Load the five-node line scenario.
3. Start the simulation.
4. Wait for running state.
5. Toggle one link.
6. Start a short traffic run.
7. Observe metrics.
8. Stop the simulation.

Do not mock the backend in the primary browser smoke test.

# 18. Acceptance script

Create `scripts/acceptance.py` or an equivalent command that performs a complete headless acceptance test.

It must:

1. Verify API health.
2. Start a clean three-node relay scenario.
3. Connect official Meshtastic clients to the three public endpoints.
4. Verify node information can be read from each.
5. Send a node-1-to-node-3 message through node 2.
6. Confirm destination delivery.
7. Disable the required relay link.
8. Confirm a subsequent message is not delivered within a bounded timeout.
9. Restore the link.
10. Confirm delivery resumes.
11. Run a short fixed-rate test.
12. Validate that generated, delivered, RF-transmission, latency, and airtime metrics are internally consistent.
13. Stop the simulation.
14. Confirm all child processes and public node listeners shut down.

The README must provide one command to run this acceptance test.

# 19. Docker and developer commands

The following user path must work:

docker compose up --build

Then:

- web UI available on port 8080;
- virtual radios available on ports 45001–45010;
- no manual Python virtual environment required;
- no manual native-firmware build required unless the Docker build performs it automatically.

Provide consistent repository commands such as:

- `make dev`
- `make test`
- `make integration-test`
- `make acceptance`
- `make lint`
- `make clean`

Equivalent task-runner commands are acceptable.

Pin frontend and backend dependency lockfiles.

Add container health checks.

Test the supported path on Linux. Also inspect and document behavior on macOS Docker Desktop, including Apple Silicon. If an upstream image lacks a required architecture, use an explicit platform fallback and document the performance implication instead of allowing a cryptic build failure.

# 20. Documentation

Write:

## README.md

Include:

- what the simulator does;
- what it does not do;
- requirements;
- one-command startup;
- ports;
- how to connect an official Meshtastic client to a selected virtual node;
- how to run the acceptance test;
- how to save and load scenarios;
- how to interpret the primary metrics;
- troubleshooting.

## docs/architecture.md

Include:

- component diagram;
- node gateway data flow;
- RF transmission data flow;
- lifecycle state machine;
- process model;
- concurrency model;
- failure handling;
- reason for not exposing daemon ports directly.

## docs/client-connections.md

Explain:

- one external client per node;
- Client API framing;
- configuration handshake handling;
- reconnect behavior;
- how internal simulator traffic and external traffic share the downstream connection.

## docs/fidelity.md

Explicitly classify what V1 models well:

- real firmware packet handling;
- firmware queueing;
- routing and relaying;
- acknowledgments;
- retries;
- duplicate suppression;
- configured directed connectivity;
- RSSI and SNR injection;
- validated collision model;
- application-load response.

Explicitly classify what V1 does not model accurately:

- exact outdoor range;
- terrain;
- buildings;
- multipath;
- antenna orientation;
- oscillator and individual-radio variation;
- local interference unless explicitly introduced;
- battery consumption;
- physical hardware driver behavior;
- exact wall-clock scheduling of a microcontroller under all conditions.

Never describe V1 as a complete RF propagation simulator.

# 21. Explicit V1 non-goals

Do not add these unless they are necessary to complete a required feature:

- geographic maps;
- terrain or elevation data;
- path-loss calculations;
- mobile-node animation;
- GPS route replay;
- discrete-event simulation;
- faster-than-real-time simulation;
- more than 10 nodes;
- multiple clients attached to one node;
- BLE emulation;
- serial-device emulation;
- MQTT infrastructure;
- Wi-Fi HaLow, XBee, SiK, or other radio protocols;
- several physical RF networks in one scenario;
- multiple Meshtastic logical channels;
- user authentication;
- remote multi-user hosting;
- Kubernetes;
- PostgreSQL;
- polished visual packet-route animation;
- automatic capacity sweeps;
- exact battery or power modeling.

Keep interfaces modular enough that map-based links, replay, and a discrete-event backend can be added later.

# 22. Implementation order

Implement in this order:

1. Upstream investigation and version pinning.
2. One-node gateway spike.
3. Headless process supervisor.
4. Two-node deterministic RF forwarding.
5. Three-node relay integration test.
6. Collision verification.
7. Versioned scenario model.
8. Simulator lifecycle API.
9. Runtime link updates.
10. Traffic generator.
11. Metrics and persistence.
12. WebSocket event feed.
13. Minimal web UI.
14. Docker packaging.
15. Full test suite and acceptance script.
16. Documentation and cleanup.

Do not start with the visual topology editor. The client gateway and real firmware data path are the primary technical risks.

# 23. Definition of done

V1 is complete only when all of the following are true:

- `docker compose up --build` starts the product.
- The UI shows five virtual radios by default.
- Five different programs could theoretically connect to five different public node ports at the same time.
- At least two official clients have been tested simultaneously against different nodes.
- The controller remains able to observe and inject simulated RF while those clients are connected.
- A three-node relay case works using real firmware.
- Runtime link removal and restoration work.
- Collision behavior has an automated test and is accurately labeled.
- A fixed-rate traffic run produces saved metrics.
- Start and stop cleanup is bounded and leak-free.
- All versions are pinned.
- Unit, integration, browser smoke, and acceptance tests pass.
- Documentation clearly describes fidelity limitations.
- No production behavior is implemented by placeholder mocks.
- There are no silent fallbacks that make results look more realistic than they are.

# 24. Final response

When implementation is complete, report:

1. Architecture selected.
2. Exact upstream commits, package versions, and firmware image digest.
3. How the per-node gateway works.
4. How collision behavior was implemented and verified.
5. Commands used to start and test the application.
6. Acceptance-test results.
7. Supported host platforms.
8. Remaining V1 limitations.
9. Files or modules that are likely extension points for V2.

Be direct about anything that did not pass. Do not claim a feature works unless it was executed in a test.
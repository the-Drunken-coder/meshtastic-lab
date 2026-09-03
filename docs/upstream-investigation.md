# Upstream investigation

Investigated on 2026-09-02. `PROJECT_BRIEF.md` remained unchanged during this work. Its starting SHA-256 is `03f590fbada752fa59a50e9e917c3584da4d5e1c1831c946947b70e5bd3f0561`.

## Exact revisions

| Component | Revision used | Notes |
| --- | --- | --- |
| Meshtasticator | `17ceb8231079d87b070abc6132181e4c6b20202d` | Current `meshtastic/Meshtasticator` `master` at investigation time. |
| Firmware source inspected | `8c4b69c3bc16925420afa3b8a52b9871d2feaffc` | Current `meshtastic/firmware` `develop` at investigation time. |
| Firmware runtime | `2.7.26.54e0d8d` at `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb` | The pinned official image was built from this firmware revision. |
| Firmware protobuf submodule | `6b1ded439633cd03d4af85b44231b91d1d106278` | Recorded from the runtime firmware commit. |
| Official firmware image | `meshtastic/meshtasticd@sha256:23e92b1331a3a471eaef0c63cbca4365ca40b3111a9781cfdbe5a5114e5773d4` | OCI index. Never referenced as `latest` by the application. |
| Meshtastic Python source | `0539a9600fc6c15eda398494ffb0309322cfb089` | Current `meshtastic/python` `master` at investigation time. |
| Meshtastic Python package | `2.7.11` | Pinned for Meshtastic Lab and its acceptance client. Meshtasticator's own `meshtastic~=2.6.1` resolved to `2.6.4` for the upstream run. |
| Protobuf Python runtime | `6.33.6` | Locked application dependency. Generated Python definitions ship in `meshtastic==2.7.11`. |

The OCI index contains Linux images for `amd64`, `arm64`, `arm/v7`, and `riscv64`. Meshtastic Lab supports Linux `amd64` and `arm64`. Docker Desktop on Apple Silicon uses the native `arm64` manifest, so it does not require x86 emulation.

## Live two-node run

I cloned the official Meshtasticator revision, installed its declared requirements in Python 3.13, pulled the official daemon image, and ran:

```text
.venv/bin/python interactiveSim.py 2 -d
```

Both native nodes completed their Client API setup. `nodes 0 1` returned node databases from both interfaces. `broadcast 0 upstream-two-node-check` entered the node 0 firmware queue through the official Python client. The simulator then shut down its child container. The upstream shutdown printed a harmless second-remove warning because its auto-remove container was already gone.

## Simulated RF data path

`src/platform/portduino/SimRadio.cpp` is the firmware boundary. On transmit, `SimRadio::startSend()` keeps the real `MeshPacket` fields, wraps its decoded payload in `Compressed`, changes the outer port number to `SIMULATOR_APP` (`69`), and emits the packet through `PhoneAPI`. Ciphertext stays ciphertext by using `UNKNOWN_APP` as a sentinel inside `Compressed`.

The simulator receives that ordinary `FromRadio.packet`, chooses receivers, copies the same mesh fields, adds the directed link's RSSI and SNR, wraps it in `ToRadio.packet`, and sends it to each receiving daemon. `SimRadio::unpackAndReceive()` restores the original port and payload, then calls the real radio receive path. The simulator must not decrypt, route, acknowledge, retry, or rebroadcast the application packet.

Meshtasticator follows this model in `lib/interactive.py`, but its optional forwarding socket is not safe enough for this product. It assumes read boundaries, uses multiple send sites, has no bounded queues, and only handles one special forwarded node. Meshtastic Lab uses the upstream RF representation and replaces that forwarding shim with a framed per-node gateway.

## Framing and configuration synchronization

TCP uses `0x94 0xC3`, a two-byte big-endian payload length, then one serialized `ToRadio` or `FromRadio`. The firmware maximum is 512 protobuf bytes. `StreamAPI` resynchronizes after garbage, accepts fragmented input, and processes several frames from a single read.

The official Python client sends 32 `0xC3` wake and resynchronization bytes, starts its reader, then sends `ToRadio.want_config_id`. `PhoneAPI::handleStartConfig()` restarts a resumable state machine. It emits `my_info`, metadata, node records, channels, configuration and module configuration, then a matching `config_complete_id`. A reconnect uses a new TCP connection and repeats this exchange.

Each gateway keeps one connection to its daemon for the node's lifetime. The gateway forwards an external client's `want_config_id` on that connection and preserves the returned frames byte for byte. Controller writes use the same bounded serialized downstream queue. `SIMULATOR_APP` frames go to the RF controller and are not exposed as application traffic to the external client.

## TCP client behavior

The current native server stores one `openAPI`. A new daemon-side TCP connection force-closes the previous one. This proves why the application cannot connect the controller and an external client separately. Meshtastic Lab exposes one public client per gateway and rejects a second public connection before it can disturb the daemon connection.

Different gateways own different daemons, so clients may connect to different virtual nodes at the same time. An external disconnect leaves the downstream daemon connection and controller running. The next external connection performs a fresh normal configuration exchange without restarting the daemon.

## Timing and collision behavior

Without `USERPREFS_SIMRADIO_EMULATE_COLLISIONS`, injected receptions complete immediately. With the flag, `SimRadio` retains the receiving packet for its calculated airtime. A second overlapping reception marks the receiver's current packet bad, increments `rxBad`, and drops both. A node already transmitting may also reject an overlapping reception after its preamble window.

The pinned stock image does not contain the collision log strings guarded by `USERPREFS_SIMRADIO_EMULATE_COLLISIONS`. Meshtastic Lab therefore builds the native binary from exact firmware commit `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb` with `USERPREFS_SIMRADIO_EMULATE_COLLISIONS=true`. The build and capability probe fail closed. The API never labels a run `native` unless the collision integration probe passes.

The medium must inject overlapping frames without serializing their simulated receive windows. The hidden-terminal test starts node 1 and node 3 transmissions on a controlled schedule into node 2, then reads firmware local statistics and packet outcomes. The fixed scenario seed makes the traffic schedule repeatable. It does not make host scheduling cycle-exact.

## Compatibility assumptions

- Firmware `2.7.26` and Python client `2.7.11` use the same public Client API envelope and framing limit.
- The gateway treats protobuf parsing as classification only. It forwards ordinary frames unchanged, which preserves unknown fields added by compatible future firmware.
- Controller-created `ToRadio` messages use the generated protobufs bundled with the pinned Python client. A protobuf mismatch that prevents parsing or the config handshake is a startup failure.
- The simulator supports one common RF profile and one primary logical channel. It does not translate between protobuf schema generations.

## Primary sources inspected

- `meshtastic/Meshtasticator`: `interactiveSim.py`, `lib/interactive.py`, and `INTERACTIVE_SIM.md`
- `meshtastic/firmware`: `src/platform/portduino/SimRadio.cpp`, `src/mesh/StreamAPI.cpp`, `src/mesh/PhoneAPI.cpp`, and `src/mesh/api/ServerAPI.*`
- `meshtastic/python`: `meshtastic/stream_interface.py`, `meshtastic/tcp_interface.py`, `meshtastic/mesh_interface.py`, and `pyproject.toml`
- `meshtastic/protobufs`: `mesh.proto` and `portnums.proto`

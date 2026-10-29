# Project context and continuation

Reconciled on 2026-10-29 against main `bc267395df0d7c34c31c249d1c66e90d3890bff9`.

Meshtastic Lab runs 2–10 isolated native Meshtastic firmware processes, ordinary client gateways, an explicit directed-link medium, an API and a web console. The firmware creates and handles packets. The lab controls configured hearability and offered traffic.

## Useful evidence

This is a firmware-behavior environment: queue admission, encryption, relaying, duplicate suppression, acknowledgments, retries and collision handling come from the pinned native firmware path. The [fidelity document](fidelity.md) defines the exact build provenance and limits.

The fixture is reproducible within declared inputs and seed streams. Host scheduling is not cycle-exact MCU execution. RSSI and SNR are injected metadata, not outputs derived from geographic distance. A scenario therefore does not predict outdoor range, buildings, foliage, terrain, antenna installation or battery performance.

The included LoRa behavior cannot validate FLRC RF performance or the later standalone W12 firmware. [The older coordinated Meshtastic FLRC forks](https://github.com/the-Drunken-coder/meshtastic-workspace) also have their own software and hardware acceptance record.

## Continuation map

| Location                                          | Purpose                                                      |
| ------------------------------------------------- | ------------------------------------------------------------ |
| [README](../README.md)                            | Setup, supported hosts, operating commands and metrics       |
| [Architecture](architecture.md)                   | Process and service responsibilities                         |
| [Client connections](client-connections.md)       | Standard gateway/client boundaries                           |
| [Fidelity and metric interpretation](fidelity.md) | Collision provenance, timing, delivery and acceptance limits |
| [System bench](../experiments/README.md)          | Repeatable comparisons using the running Compose API         |
| [Workloads](../experiments/workloads/)            | Reusable named flow and preset inputs                        |
| [Experiment reports](../experiments/)             | Human-readable historical findings alongside the runner      |

The committed reports and workload definitions already form the available handoff. No separate missing external data package was recovered for this update. Complete generated exports belong under the existing ignored `data/experiments/` output. Preserve failed and cancelled trials alongside completed ones when evaluating a run.

## Interpret results correctly

- Generated, submitted and submission-failed messages describe different boundaries.
- Unique message delivery differs from receiver delivery count for a broadcast.
- Destination delivery differs from acknowledgment success. Broadcasts have no direct-message acknowledgment denominator.
- RF transmission and airtime count transmitter events once, not once per receiver.
- Source phasing changes contention while preserving per-source offered traffic.
- Live latency percentiles use a bounded window; completed exports use the complete run and retain sample-count requirements.
- Partial local-stat samples must remain marked incomplete rather than being presented as zero bad receptions.

Those definitions matter when comparing presets or claiming a telemetry workload fits. The relevant acceptance result includes the exact runtime/build provenance, scenario, seed, generated records, failures and measured service, not only a chart screenshot.

## Next useful experiment

Choose a bounded Meshtastic question: offered load, source timing, acknowledgment policy, relay behavior or connectivity change. Start with the existing workload runner, freeze the fixture and compare repeated trials. Keep control changes separate from traffic changes. Use hardware measurements for claims that the native fixture cannot establish.

This update adds context and navigation only. It does not change the firmware pin, RF model, workload acceptance criteria or recorded experiment results.

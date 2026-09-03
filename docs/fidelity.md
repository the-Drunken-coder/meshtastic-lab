# Fidelity and metric interpretation

Meshtastic Lab V1 is a firmware behavior test environment with an explicit connectivity graph. It is not a complete RF propagation simulator.

## Modeled well

- Native firmware packet construction and encryption
- Firmware queues and channel-access timing
- Flood routing, relay decisions, acknowledgments, retry behavior, and duplicate suppression
- The configured directed hearability graph
- Per-link RSSI and SNR injection
- Native simulated-radio overlap handling with the collision build preference enabled
- Client API configuration, NodeDB synchronization, reconnects, and ordinary application traffic
- Firmware response to a controlled real-time offered application load

The deterministic seed controls traffic destination and identifier choices through separate random streams, so identifier retries cannot shift the destination sequence. Packet identifiers remain reserved for the current run and enter a five-minute quarantine afterward to reject late acknowledgments. The quarantine is capped at 36,000 identifiers per source, equal to the largest supported one-hour run at 600 messages per minute. Firmware queue status, keyed by source and packet ID, decides whether a request is submitted or submission failed. Traffic requests identify their local source so the Phone API returns pre-transmission rate-limit routing errors; such an error overrides queue status because the pinned firmware reports a zero queue result for that rejection. MeshService still normalizes accepted packets before RF transmission. It does not make host thread scheduling cycle-exact.

## Not modeled accurately

- Exact indoor or outdoor range
- Terrain, buildings, foliage, multipath, diffraction, or fading
- Antenna pattern, placement, cable loss, or orientation
- Oscillator and individual-radio variation
- Unconfigured local interference or noise-floor changes
- Battery consumption and power-management behavior
- Physical radio and microcontroller driver timing
- Exact wall-clock scheduling of an embedded microcontroller under every load

RSSI and SNR are metadata supplied to enabled receiver firmware. They are not calculated from distance. A disabled link injects nothing.

## Native collisions

The default image builds firmware commit `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb` with `USERPREFS_SIMRADIO_EMULATE_COLLISIONS=1`. Under that preference, `SimRadio` retains a receive for its calculated airtime. A second overlapping receive marks the current receive bad, increments the firmware `rxBad` statistic, and drops the overlap. A receive can also collide with an active transmission after its preamble window.

The Docker build checks for the collision-only firmware log string before creating the runtime marker. It also records the resolved firmware commit, collision-patch SHA-256, final binary SHA-256, build architecture, client-library version, and the separately named upstream base-image digest. Capabilities and completed results read that generated metadata instead of identifying the custom executable by the stock image digest. Runtime start fails if the collision marker is absent. Traffic runs request local statistics immediately before generation and after the drain phase or cancellation, then persist the available per-node `rxBad` deltas. Sampling retries missing nodes under one deadline. A partial final sample does not fail the traffic run and is identified in the result. The hidden-terminal integration test drives two non-hearing transmitters into one receiver and verifies the native bad-reception outcome. There is no approximate fallback in V1.

## Airtime

`backend/app/metrics/airtime.py` is a direct independent implementation of the equation used by pinned `SimRadio::getPacketTime`. For bandwidth `BW`, spreading factor `SF`, coding-rate denominator value `CR`, preamble `P`, and packet length `PL`:

```text
Tsymbol = 2^SF / BW
DE = 1 when Tsymbol > 16 ms, otherwise 0
Tpreamble = (P + 4.25) × Tsymbol
payloadSymbols = 8 + max(ceil((8PL - 4SF + 28 + 16) / (4(SF - 2DE))) × CR, 0)
airtime = Tpreamble + payloadSymbols × Tsymbol
```

The packet length is recovered from the actual firmware-created simulated frame. Encrypted packets count ciphertext plus the 16-byte mesh header. Decoded simulator wrappers are unwrapped before measuring. Tests cover every supported modem preset and reference values taken from the upstream function.

One RF transmitter event contributes airtime once, regardless of receiver count. Receiver delivery, duplicate reception, goodput, raw packet size, RF transmissions, relay transmissions, and per-node transmit airtime remain distinct measures.

After the final request, traffic enters a bounded drain phase. Direct runs wait for each firmware-accepted message to deliver and, when requested, acknowledge or return a routing error. Broadcast runs wait for correlated activity to remain quiet. Quiet and absolute deadlines derive from the largest generated packet airtime, hop limit, and the pinned firmware's origin and intermediate retry budgets, including contention and processing delay, with a hard upper bound.

## Delivery and latency

Built-in payloads include a compact run UUID and sequence. Generation uses a monotonic clock. Direct delivery occurs only when the intended destination firmware exposes that matching application packet. Broadcast accounting counts unique receiving nodes other than the source against the applicable receiver set. Firmware clears broadcast acknowledgment requests, so acknowledgment success is unavailable for broadcasts. Direct acknowledgment success uses firmware-accepted submissions as its denominator and remains separate from destination delivery.

Median appears with one or more latency samples. P95 requires 20 samples and P99 requires 100. Insufficient samples are unavailable, not zero. Live polling reports bounded incremental counters; exact latency percentiles are finalized when a run becomes terminal. Completed exports include the exact runtime scenario snapshot, build metadata, collision model, timestamps, seed, generated records, aggregate metrics, and failures.

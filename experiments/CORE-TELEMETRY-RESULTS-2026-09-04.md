# Core telemetry preset sweep, 2026-09-04

## Outcome

None of the nine modem presets met the tested service target. `SHORT_TURBO` was the best result, but it still delivered only 91 of 108 telemetry messages, acknowledged 88, and delivered only 39.8% before that source's next five-second interval. The remaining presets degraded as packet airtime increased. No preset had excess bandwidth for this workload.

The application workload was more demanding than its 112 messages per minute suggests. Native firmware flooding, acknowledgments, and retries expanded those 112 messages into as many as 1,257 RF transmissions in one trial. Between 50.8% and 62.4% of observed RF transmissions were relays, even though every destination was one physical hop away in the configured full mesh.

## Workload

- Ten native firmware nodes in a full directed mesh at RSSI -85 dBm and SNR 8 dB
- Fresh radio state for every preset, US region, frequency slot 20, hop limit 4
- 64-byte direct text with acknowledgments requested
- `telemetry-to-core`: nodes 2 through 10 sent to node 1 every five seconds, evenly staggered
- `core-commands`: node 1 sent to a deterministic random non-core node every fifteen seconds
- Sixty seconds of generation, followed by the same bounded drain policy for every preset
- One deterministic trial per preset, seed 20260904

The run generated 108 telemetry messages and four commands per preset. For this report, a message is "within interval" when it reached its destination before that flow's next scheduled send. That is a queue-health threshold, not a claim about the application's actual SLA.

## Application results

| Preset | Nominal 80 B airtime | Overall delivered | Telemetry delivered | Telemetry ACK | Telemetry within 5 s | Telemetry p95 | Commands delivered / ACK | Commands within 15 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SHORT_TURBO | 37 ms | 95/112 (84.8%) | 91/108 | 81.5% | 39.8% | 13.5 s | 4/4 / 4 | 100% |
| SHORT_FAST | 74 ms | 88/112 (78.6%) | 85/108 | 66.7% | 31.5% | 16.2 s | 3/4 / 2 | 75% |
| SHORT_SLOW | 133 ms | 71/112 (63.4%) | 69/108 | 44.4% | 25.0% | 22.8 s | 2/4 / 2 | 50% |
| MEDIUM_FAST | 242 ms | 52/112 (46.4%) | 52/108 | 28.7% | 9.3% | 40.1 s | 0/4 / 0 | 0% |
| MEDIUM_SLOW | 447 ms | 40/112 (35.7%) | 40/108 | 28.7% | 7.4% | 112.3 s | 0/4 / 0 | 0% |
| LONG_TURBO | 594 ms | 46/112 (41.1%) | 45/108 | 20.6% | 8.3% | 92.4 s | 1/4 / 1 | 25% |
| LONG_FAST | 829 ms | 35/112 (31.3%) | 34/108 | 11.7% | 0.9% | 147.4 s | 1/4 / 1 | 25% |
| LONG_MODERATE | 2,805 ms | 27/112 (24.1%) | 27/108 | 7.6% | 0.9% | 311.1 s | 0/4 / 0 | 0% |
| LONG_SLOW | 5,120 ms | 17/112 (15.2%) | 17/108 | 5.7% | 0.0% | unavailable | 0/4 / 0 | 0% |

`LONG_SLOW` has no p95 because fewer than 20 telemetry deliveries survived. Command counts are only four per preset, so they are descriptive rather than statistically stable.

## RF behavior

| Preset | RF transmissions | Relay share | Failed receptions | Max-retry drops | Aggregate airtime / wall time | Trial elapsed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SHORT_TURBO | 1,206 | 58.8% | 4,008 | 39 | 32.0 s / 173.4 s (18.5%) | 173.4 s |
| SHORT_FAST | 1,257 | 62.4% | 4,348 | 60 | 71.8 s / 177.8 s (40.4%) | 177.8 s |
| SHORT_SLOW | 961 | 59.3% | 3,201 | 87 | 104.7 s / 186.1 s (56.2%) | 186.1 s |
| MEDIUM_FAST | 789 | 55.6% | 2,632 | 110 | 168.6 s / 202.6 s (83.2%) | 202.6 s |
| MEDIUM_SLOW | 695 | 55.3% | 2,336 | 93 | 278.2 s / 234.2 s (118.8%) | 234.2 s |
| LONG_TURBO | 699 | 56.1% | 2,232 | 107 | 371.4 s / 239.4 s (155.2%) | 239.4 s |
| LONG_FAST | 477 | 53.0% | 1,697 | 84 | 371.1 s / 297.6 s (124.7%) | 297.6 s |
| LONG_MODERATE | 360 | 55.6% | 1,230 | 84 | 903.3 s / 360.2 s (250.8%) | 360.2 s |
| LONG_SLOW | 197 | 50.8% | 724 | 56 | 911.5 s / 359.7 s (253.4%) | 359.7 s |

Aggregate airtime sums simultaneous transmissions from every radio. It can exceed 100% because overlapping transmissions are possible and are precisely what the native collision model rejects. The reduction in RF-transmission count on slower profiles is not efficiency. It means fewer attempts fit before queues, retries, and the five-minute drain ceiling stop useful progress.

## Where it faltered

1. The telemetry fan-in created a hot shared channel and a hot core. Every source requested reliable delivery on a five-second cadence.
2. Flood routing amplified one-hop direct messages. The trials spent roughly half of their RF transmissions relaying packets that did not need another physical hop in this topology.
3. Retries increased congestion instead of recovering it. Every preset recorded `MAX_RETRANSMIT` drops, and acknowledgment success declined from 81.5% on `SHORT_TURBO` to 5.7% on `LONG_SLOW`.
4. Slower presets accumulated severe queues. `LONG_MODERATE` and `LONG_SLOW` reached the five-minute drain ceiling, with delivered telemetry arriving as late as 315 and 319 seconds respectively.
5. Firmware admission eventually failed as well: one request on `SHORT_SLOW`, two on `LONG_TURBO`, five on `LONG_FAST`, and three each on `LONG_MODERATE` and `LONG_SLOW` were not accepted.

The host did not look like the limiting resource. All nine trials completed without a simulator failure, memory remained roughly 90 to 106 MiB, and the maximum measured event-loop lag was 14 ms. CPU had brief multi-core spikes, including one 600% sample, but median traffic-run CPU ranged from 5% to 19%.

## What I would change

For this topology, I would first set the nine leaf radios to a non-relaying client role and retain routing only where the physical topology needs it. Then I would stop requesting a mesh ACK for every periodic telemetry sample, batch or coalesce sensor state, and reserve acknowledged traffic for commands or state transitions. If every sample truly must be reliable, the five-second interval should be relaxed or the radios split across channels.

`SHORT_TURBO` is the only sensible starting preset from this sweep, assuming its shorter link budget is acceptable in the field. The simulator does not calculate range, fading, foliage, terrain, antenna loss, or external interference, so this result cannot select a field preset by itself.

The next experiment should find the knee rather than repeat this one overloaded point: sweep telemetry intervals of 5, 10, 15, 30, and 60 seconds with three trials each, compare acknowledged and unacknowledged telemetry, and repeat with leaf relaying disabled. After that, run the winning settings on line and star topologies and at weaker RSSI/SNR values.

## Harness assessment

The new workload file makes this sweep a single command:

```bash
uv run python experiments/run_system_bench.py \
  --workload experiments/workloads/core-telemetry-all-presets.json
```

The harness successfully started ten native nodes for every preset, merged both flows on one monotonic schedule, waited for terminal drain, captured per-message evidence, and wrote one immutable artifact per preset plus a cross-run summary. The complete sweep took 46 minutes 21 seconds. It is now easy to change presets, rates, source sets, source timing, destinations, payload size, duration, topology, hop limit, slot, seed, and trial count in JSON.

The run also exposed two reporting defects. Mixed per-flow timing was labeled with the unused top-level default, and per-flow p95 did not enforce the project's 20-sample minimum. Both are fixed after this capture. The harness now reports mixed timing accurately, withholds p95 below 20 deliveries, records maximum latency, and calculates the fraction delivered before the next flow interval. The raw captured messages remain unchanged.

This was a cold-start sweep. The 21 to 23 second startup window observed 70 of 90 graph-connected node pairs on `SHORT_TURBO`, but only 6 of 90 on `LONG_SLOW`. That is realistic for commissioning behavior but confounds pure steady-state capacity. A future workload option should support an explicit pre-traffic settle condition or duration, along with per-flow acknowledgment policy and declarative link RSSI/SNR, before treating the harness as a full radio-planning tool.

## Evidence

- Workload: `experiments/workloads/core-telemetry-all-presets.json`
- Results: `data/experiments/20260904T165925Z/summary.json` and the nine adjacent per-preset artifacts
- Native firmware commit: `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb`
- Simulator capture revision: `01032d88a5203639a926df14261c3fa1e289b02e`
- Native collision model and failed-reception metrics were available and complete for every trial

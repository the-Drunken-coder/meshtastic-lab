# Five-radio core telemetry sweep, 2026-09-04

## Outcome

Reducing the mesh from ten radios to five moved the fast short-range presets into a useful operating region. In the initial trial, `SHORT_FAST` delivered and acknowledged every application message, all 48 telemetry messages arrived before the source's next five-second send, telemetry p95 was 0.83 seconds, and the slowest telemetry delivery was 1.27 seconds.

`SHORT_TURBO` also delivered and acknowledged everything, but three telemetry messages missed the five-second interval and the slowest took 13.73 seconds. Twenty fresh follow-up runs did not reproduce a stable `SHORT_FAST` advantage. Across ten trials per short preset, Turbo delivered 520/520 application messages and Fast delivered 518/520. The timing winner reversed when command timing changed. `SHORT_SLOW` and every slower preset lost messages or accumulated unacceptable delay in the initial sweep.

## Workload

- Five native firmware nodes in a full directed mesh at RSSI -85 dBm and SNR 8 dB
- Node 1 was the core; nodes 2 through 5 were telemetry sources
- Each telemetry source sent a 64-byte acknowledged direct message every five seconds
- Node 1 sent one acknowledged direct command to a deterministic random node every fifteen seconds
- Sixty seconds of generation, 48 telemetry messages plus four commands per preset
- Fresh state, US region, frequency slot 20, hop limit 4, seed 20260904
- One trial for each of the seven requested modem presets

"Within interval" means a message reached its destination before that flow's next scheduled send. It is a queue-health test rather than a declared application SLA.

## Results

| Preset | Overall delivered | Telemetry delivered / ACK | Telemetry within 5 s | Median | P95 | Maximum | Commands delivered / ACK | Assessment |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SHORT_TURBO | 52/52 (100%) | 48/48 / 48 | 45/48 (93.8%) | 0.49 s | 4.42 s | 13.73 s | 4/4 / 4 | Reliable, marginal timing |
| SHORT_FAST | 52/52 (100%) | 48/48 / 48 | 48/48 (100%) | 0.49 s | 0.83 s | 1.27 s | 4/4 / 4 | Meets with observed headroom |
| SHORT_SLOW | 49/52 (94.2%) | 46/48 / 46 | 32/48 (66.7%) | 0.97 s | 14.57 s | 15.87 s | 3/4 / 3 | Fails |
| MEDIUM_FAST | 44/52 (84.6%) | 41/48 / 39 | 25/48 (52.1%) | 2.26 s | 16.75 s | 17.19 s | 3/4 / 3 | Fails |
| MEDIUM_SLOW | 34/52 (65.4%) | 31/48 / 25 | 8/48 (16.7%) | 13.65 s | 44.45 s | 49.42 s | 3/4 / 3 | Fails |
| LONG_TURBO | 37/52 (71.2%) | 34/48 / 28 | 12/48 (25.0%) | 13.08 s | 45.82 s | 54.60 s | 3/4 / 2 | Fails |
| LONG_FAST | 30/52 (57.7%) | 29/48 / 21 | 7/48 (14.6%) | 25.46 s | 72.71 s | 74.78 s | 1/4 / 1 | Fails |

The `SHORT_SLOW` latency distribution is notably bimodal. Its median was below one second, but retries pushed its tail past fifteen seconds and it lost three application messages. Median latency alone would make this preset look much healthier than it was.

## RF behavior

| Preset | RF transmissions | Relay share | Failed receptions | Max-retry drops | Aggregate airtime / wall time | Trial elapsed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SHORT_TURBO | 208 | 12.5% | 72 | 0 | 5.3 s / 73.3 s (7.2%) | 73.3 s |
| SHORT_FAST | 187 | 8.0% | 53 | 0 | 9.7 s / 67.8 s (14.4%) | 67.8 s |
| SHORT_SLOW | 313 | 28.1% | 287 | 7 | 30.2 s / 78.2 s (38.6%) | 78.2 s |
| MEDIUM_FAST | 323 | 28.8% | 315 | 22 | 58.6 s / 93.6 s (62.6%) | 93.6 s |
| MEDIUM_SLOW | 330 | 37.9% | 370 | 41 | 120.7 s / 233.2 s (51.7%) | 233.2 s |
| LONG_TURBO | 335 | 37.3% | 349 | 40 | 155.3 s / 238.6 s (65.1%) | 238.6 s |
| LONG_FAST | 274 | 37.6% | 320 | 49 | 188.2 s / 296.5 s (63.5%) | 296.5 s |

`SHORT_FAST` needed fewer RF transmissions, produced fewer relays and failed receptions, and completed faster than `SHORT_TURBO` in this trial. The workload schedule is deterministic, but native firmware timing is not cycle-deterministic, so that difference needs repeated trials before it is treated as a stable ranking.

The host remained comfortably below the RF bottleneck: median container CPU was 4% to 13%, peak memory was at most 102.3 MiB, and maximum measured event-loop lag was 4 ms.

## Effect of reducing node count

| Preset | Ten-radio delivery | Five-radio delivery | Ten-radio telemetry within 5 s | Five-radio telemetry within 5 s |
| --- | ---: | ---: | ---: | ---: |
| SHORT_TURBO | 84.8% | 100% | 39.8% | 93.8% |
| SHORT_FAST | 78.6% | 100% | 31.5% | 100% |
| SHORT_SLOW | 63.4% | 94.2% | 25.0% | 66.7% |
| MEDIUM_FAST | 46.4% | 84.6% | 9.3% | 52.1% |
| MEDIUM_SLOW | 35.7% | 65.4% | 7.4% | 16.7% |
| LONG_TURBO | 41.1% | 71.2% | 8.3% | 25.0% |
| LONG_FAST | 31.3% | 57.7% | 0.9% | 14.6% |

The reduction was more than proportional because it removed both origin traffic and relay/retry amplification. Relay share fell from roughly 51% to 62% in the ten-radio trials to 8% to 38% here.

## Interpretation and follow-up

The initial `SHORT_FAST` result was a useful run, not a stable preset ranking. Five aligned repeats favored Turbo, while five repeats with the core commands offset from telemetry favored Fast. Across both schedules, Turbo's telemetry on-time rate was 94.17% and Fast's was 93.12%. The uncertainty interval included either preset winning.

The complete follow-up is recorded in `experiments/SHORT-PRESET-REPEATS-2026-09-04.md`. The next useful experiment should hold startup convergence constant and capture native contention-window and retry-delay traces. Once the cause of the latency cascades is observable, an interval sweep can measure actual headroom. Leaf nodes should be tested as `CLIENT_MUTE`, because a full one-hop mesh does not need them to relay each other's packets.

This is still a cold-start result. Startup observed 18 of 20 graph-connected node pairs on `SHORT_FAST`, but only 6 of 20 on `LONG_FAST`. A steady-state capacity study should add an explicit convergence or settling condition before traffic generation.

## Evidence

- Workload: `experiments/workloads/core-telemetry-five-radios.json`
- Results: `data/experiments/20260904T180808Z/summary.json` and the seven adjacent per-preset artifacts
- Run time: 20 minutes 34 seconds
- Native firmware commit: `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb`
- Simulator capture revision: `01032d88a5203639a926df14261c3fa1e289b02e`
- Native collision and failed-reception metrics were available and complete for every trial

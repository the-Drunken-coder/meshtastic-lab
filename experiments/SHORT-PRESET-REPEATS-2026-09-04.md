# Five-radio short-preset repeats, 2026-09-04

## Design

- Five native firmware radios in the full-mesh scenario.
- Nodes 2 through 5 sent 64-byte acknowledged telemetry to node 1 every 5 seconds.
- Node 1 sent an acknowledged command to a deterministic random node every 15 seconds.
- Each run generated 52 application messages over 60 seconds.
- Every trial recreated native firmware, so no trial inherited queues, NodeDB entries, routes, or timers.
- Preset order was balanced as Turbo, Fast, Fast, Turbo, Turbo, Fast, Fast, Turbo, Turbo, Fast.
- Five trials per preset kept the original aligned command schedule.
- Five trials per preset used deterministic command offsets of approximately 5.876, 20.874, 35.879, and 50.876 seconds.
- Firmware commit, collision build, topology, link budget, workload seed, and radio count were unchanged.

## Results

| Schedule | Preset | Trials | App delivered | Telemetry on time | Pooled telemetry p95 | Median trial p95 | Trial p95 range | Median failed RX |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Aligned | SHORT_TURBO | 5 | 260/260 | 230/240, 95.83% | 1.601 s | 1.329 s | 0.816 to 6.040 s | 102 |
| Aligned | SHORT_FAST | 5 | 259/260 | 216/240, 90.00% | 6.513 s | 6.456 s | 0.766 to 6.950 s | 100 |
| Dephased | SHORT_TURBO | 5 | 260/260 | 222/240, 92.50% | 5.824 s | 4.266 s | 1.020 to 6.115 s | 59 |
| Dephased | SHORT_FAST | 5 | 259/260 | 231/240, 96.25% | 1.418 s | 1.110 s | 0.908 to 6.961 s | 47 |
| Combined | SHORT_TURBO | 10 | 520/520 | 452/480, 94.17% | 5.806 s | 2.813 s | 0.816 to 6.115 s | 79.5 |
| Combined | SHORT_FAST | 10 | 518/520 | 447/480, 93.12% | 6.413 s | 3.382 s | 0.766 to 6.961 s | 74.5 |

`SHORT_FAST` lost one telemetry message in the aligned condition and one core command in the dephased condition. `SHORT_TURBO` delivered every generated message.

Across all trials, `SHORT_TURBO` had 25.53 ms of observed airtime per firmware transmission and `SHORT_FAST` had 51.36 ms. Median aggregate observed transmission airtime per run was 5.228 seconds for Turbo and 10.422 seconds for Fast.

The correlation between failed receptions and per-run telemetry p95 was 0.703. The correlation between startup NodeInfo observations and p95 was -0.230.

Fast had the lower p95 in six of the ten balanced run pairs. Turbo had the lower p95 in four. The combined median per-run p95 difference was 0.569 seconds in Turbo's favor, but a trial-cluster bootstrap placed the 95% interval for that difference between 5.164 seconds in Turbo's favor and 4.005 seconds in Fast's favor. The overall on-time difference was 1.04 percentage points in Turbo's favor, with a bootstrap interval from 2.92 points in Fast's favor to 5.42 points in Turbo's favor.

## Interpretation

The original one-run `SHORT_FAST` advantage did not reproduce consistently. The aggregate winner changed when command timing changed, and both presets produced good and bad tail-latency trials. Failed receptions explain far more of the p95 movement than the preset name does.

Both presets have ample median airtime headroom for this workload, but neither gives dependable five-second tail latency under the current full-mesh retry behavior. The two observed `SHORT_FAST` losses are worth investigating, but ten trials per preset are not enough to claim a stable delivery-rate difference.

The next experiment should gate traffic on a fixed NodeInfo convergence target and record native channel utilization, contention-window, and retry-delay events. Without those traces, the simulator shows the outcome but cannot identify the internal firmware decision that caused each latency cascade.

## Evidence

- Initial seven-preset sweep: `data/experiments/20260904T180808Z/`
- Fresh aligned repeats: `data/experiments/20260904T200417Z/` through `data/experiments/20260904T201951Z/`
- Fresh dephased repeats: `data/experiments/20260904T202349Z/` through `data/experiments/20260904T203919Z/`
- Native firmware commit: `54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb`
- All runs reported the native collision model and complete failed-reception metrics.

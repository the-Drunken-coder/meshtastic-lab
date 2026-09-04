# System bench

`run_system_bench.py` drives the running Compose service through its public API. It
uses real native firmware, saves full run exports under the ignored
`data/experiments/` directory, samples container CPU and memory, and leaves the
Compose service running with the simulation stopped.

Build and start the exact checkout first:

```sh
make dev
```

In another terminal:

```sh
UV_CACHE_DIR=.uv-cache uv run python experiments/run_system_bench.py
```

For a focused comparison, select named cases and optionally override their trial
count:

```sh
UV_CACHE_DIR=.uv-cache uv run python experiments/run_system_bench.py \
  --only long-fast-ten-talker-aligned \
  --only long-fast-ten-talker-jittered \
  --trials 1
```

For a new workload, describe the radio settings, modem presets, and traffic in a
JSON file instead of editing the runner. A traffic request may contain several
named flows. Each flow has its own sources, rate, source timing, and destination
policy while sharing the run's packet type, payload size, duration,
acknowledgment policy, and seed.

```sh
UV_CACHE_DIR=.uv-cache uv run python experiments/run_system_bench.py \
  --workload experiments/workloads/core-telemetry-all-presets.json
```

The runner starts fresh native firmware for every trial, executes all flows
concurrently under one run ID, exports every generated message, reports each flow
separately, samples container resources, and leaves the simulation stopped. Use
`--trials 1-10` to override the workload's repetition count. Repetitions do not
share firmware queues, NodeDB state, routes, or timers.

The aggregate records completed, failed, and cancelled trial counts. Statistical
distributions include only completed trials, and the command exits unsuccessfully
after writing all artifacts if any trial did not complete.

The matrix covers 10-node `LONG_FAST` fan-out and contention, a 1,000-message
ingest stress, the equivalent `SHORT_FAST` contention case, a four-hop direct
route, partition and recovery, and a hidden-terminal collision. Baseline and
contention cases run three trials and the summary records median, minimum, and
maximum values. Multi-source contention compares aligned ticks with seeded,
deterministically jittered source phases. Hidden-terminal and saturation runs
stay aligned because simultaneous transmission is the behavior under test.

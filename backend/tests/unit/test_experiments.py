import asyncio
import subprocess
import threading
import time
from pathlib import Path

import pytest

import experiments.run_system_bench as system_bench
from backend.app.traffic import (
    MAX_DRAIN_SECONDS,
    SourceTiming,
    TrafficController,
    TrafficFlow,
    TrafficRunRequest,
)
from experiments.run_system_bench import (
    Experiment,
    load_workload,
    summarize_flows,
    summarize_trials,
    traffic_wait_deadline,
    workload_scenario,
)

ROOT = Path(__file__).resolve().parents[3]
WORKLOAD = ROOT / "experiments" / "workloads" / "core-telemetry-all-presets.json"
FIVE_RADIO_WORKLOAD = (
    ROOT / "experiments" / "workloads" / "core-telemetry-five-radios.json"
)


def test_core_telemetry_workload_covers_every_modem_preset() -> None:
    workload = load_workload(WORKLOAD)

    assert len(workload.modem_presets) == 9
    assert TrafficController._maximum_sequence(workload.traffic) == 112
    assert [flow.name for flow in workload.traffic.flows] == [
        "telemetry-to-core",
        "core-commands",
    ]
    scenario = workload_scenario(workload, "LONG_FAST")
    assert scenario["nodeCount"] == 10
    assert scenario["rf"]["modemPreset"] == "LONG_FAST"
    assert sum(link["enabled"] for link in scenario["links"]) == 90


def test_five_radio_workload_uses_requested_preset_subset() -> None:
    workload = load_workload(FIVE_RADIO_WORKLOAD)

    assert workload.modem_presets == [
        "SHORT_TURBO",
        "SHORT_FAST",
        "SHORT_SLOW",
        "MEDIUM_FAST",
        "MEDIUM_SLOW",
        "LONG_TURBO",
        "LONG_FAST",
    ]
    assert TrafficController._maximum_sequence(workload.traffic) == 52
    scenario = workload_scenario(workload, "SHORT_TURBO")
    assert scenario["nodeCount"] == 5
    assert sum(link["enabled"] for link in scenario["links"]) == 20


def test_flow_summary_keeps_uplink_and_downlink_results_separate() -> None:
    workload = load_workload(WORKLOAD)
    result = {
        "request": workload.traffic.model_dump(mode="json", by_alias=True),
        "generatedMessages": [
            {
                "flow": "telemetry-to-core",
                "sourceNode": "node-2",
                "destinationNode": "node-1",
                "submitted": True,
                "submissionError": None,
                "transmitted": True,
                "deliveredTo": ["node-1"],
                "acknowledged": True,
                "latencyMs": 1200,
            },
            {
                "flow": "core-commands",
                "sourceNode": "node-1",
                "destinationNode": "node-3",
                "submitted": False,
                "submissionError": "rate limited",
                "transmitted": False,
                "deliveredTo": [],
                "acknowledged": False,
                "latencyMs": None,
            },
        ],
    }

    flows = summarize_flows(result)

    assert flows["telemetry-to-core"]["deliveryRatio"] == 1
    assert flows["telemetry-to-core"]["onTimeDeliveryRatio"] == 1
    assert flows["telemetry-to-core"]["medianLatencyMs"] == 1200
    assert flows["telemetry-to-core"]["p95LatencyMs"] is None
    assert flows["telemetry-to-core"]["maximumLatencyMs"] == 1200
    assert flows["core-commands"]["admissionRatio"] == 0
    assert flows["core-commands"]["deliveryRatio"] == 0


@pytest.mark.parametrize(
    ("kind", "acknowledgment_requested"),
    [("broadcast-text", True), ("direct-text", False)],
)
def test_flow_summary_omits_ack_ratio_when_acknowledgments_are_not_expected(
    kind: str, acknowledgment_requested: bool
) -> None:
    destination = "broadcast" if kind == "broadcast-text" else "node-2"
    request = TrafficRunRequest(
        kind=kind,
        sourceNodes=["node-1"],
        fixedDestination="node-2" if kind == "direct-text" else None,
        acknowledgmentRequested=acknowledgment_requested,
    )
    result = {
        "request": request.model_dump(mode="json", by_alias=True),
        "generatedMessages": [
            {
                "flow": "default",
                "sourceNode": "node-1",
                "destinationNode": destination,
                "submitted": True,
                "submissionError": None,
                "transmitted": True,
                "deliveredTo": ["node-2"],
                "acknowledged": False,
                "latencyMs": 100,
            }
        ],
    }

    assert summarize_flows(result)["default"]["acknowledgmentSuccessRatio"] is None


def test_traffic_wait_deadline_covers_duration_phase_offset_and_drain() -> None:
    request = TrafficRunRequest(
        flows=[
            TrafficFlow(
                name="slow-jittered",
                sourceNodes=["node-1"],
                messagesPerMinute=0.1,
                sourceTiming=SourceTiming.DETERMINISTIC_JITTER,
            )
        ],
        durationSeconds=3600,
    ).model_dump(mode="json", by_alias=True)

    assert traffic_wait_deadline(request) == 3600 + 600 + MAX_DRAIN_SECONDS + 30


@pytest.mark.asyncio
async def test_resource_sampler_bounds_stalled_docker_stats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invoked = threading.Event()
    observed_timeouts: list[float] = []

    def stalled_docker_stats(command: list[str], **kwargs: object) -> None:
        timeout = float(kwargs["timeout"])
        observed_timeouts.append(timeout)
        invoked.set()
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(system_bench.subprocess, "run", stalled_docker_stats)
    stop = asyncio.Event()
    samples: list[system_bench.ResourceSample] = []
    sampler = asyncio.create_task(system_bench.sample_resources(samples, stop, time.monotonic()))

    assert await asyncio.to_thread(invoked.wait, 1)
    stop.set()
    await asyncio.wait_for(sampler, timeout=2)

    assert observed_timeouts == [system_bench.DOCKER_STATS_TIMEOUT_SECONDS]
    assert samples == []


@pytest.mark.asyncio
async def test_traffic_waiter_polls_the_created_run() -> None:
    requested_paths: list[str] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"state": "COMPLETED", "phase": "TERMINAL"}

    class FakeClient:
        async def get(self, path: str) -> FakeResponse:
            requested_paths.append(path)
            return FakeResponse()

    samples: list[dict[str, object]] = []
    result = await system_bench.wait_for_traffic(
        FakeClient(),  # type: ignore[arg-type]
        "created-run",
        1,
        samples,  # type: ignore[arg-type]
    )

    assert result["state"] == "COMPLETED"
    assert requested_paths == ["/api/traffic/runs/created-run"]


@pytest.mark.asyncio
async def test_polling_deadlines_include_pending_http_requests() -> None:
    class SlowClient:
        async def get(self, _path: str) -> None:
            await asyncio.sleep(1)

    with pytest.raises(TimeoutError, match="timed out waiting for STOPPED"):
        await system_bench.wait_for_state(
            SlowClient(),  # type: ignore[arg-type]
            "STOPPED",
            0.01,
        )

    with pytest.raises(TimeoutError, match="traffic run slow-run did not finish"):
        await system_bench.wait_for_traffic(
            SlowClient(),  # type: ignore[arg-type]
            "slow-run",
            0.01,
            [],
        )


@pytest.mark.asyncio
async def test_failed_experiment_awaits_cancelled_link_schedule(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    link_started = asyncio.Event()
    link_finished = asyncio.Event()

    async def fake_link_schedule(*_args) -> None:
        link_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            link_finished.set()

    async def failing_wait(*_args):
        await link_started.wait()
        raise RuntimeError("traffic capture failed")

    async def fake_resource_sampler(_samples, stop, _started) -> None:
        await stop.wait()

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"runId": "failed-run"}

    class FakeClient:
        async def post(self, *_args, **_kwargs) -> FakeResponse:
            return FakeResponse()

    monkeypatch.setattr(system_bench, "apply_link_schedule", fake_link_schedule)
    monkeypatch.setattr(system_bench, "wait_for_traffic", failing_wait)
    monkeypatch.setattr(system_bench, "sample_resources", fake_resource_sampler)
    request = TrafficRunRequest(
        sourceNodes=["node-1"],
        durationSeconds=1,
    ).model_dump(mode="json", by_alias=True)

    with pytest.raises(RuntimeError, match="traffic capture failed"):
        await system_bench.run_experiment(
            FakeClient(),  # type: ignore[arg-type]
            Experiment("cleanup", request),
            tmp_path,
            1,
        )

    assert link_finished.is_set()


def test_trial_summary_reports_mixed_flow_timing() -> None:
    workload = load_workload(WORKLOAD)
    request = workload.traffic.model_dump(mode="json", by_alias=True)
    experiment = Experiment("core-telemetry", request)
    artifact = {
        "result": {
            "state": "COMPLETED",
            "failedReceptionMetricsComplete": True,
            "submitted": 0,
            "delivered": 0,
            "metrics": {},
        },
        "elapsedSeconds": 0,
        "resources": {},
        "flows": {},
    }

    summary = summarize_trials(experiment, [artifact])

    assert summary["sourceTiming"] == "mixed"


def test_trial_summary_excludes_incomplete_runs_from_distributions() -> None:
    experiment = Experiment("completion-aware", {"sourceTiming": "aligned"})

    def artifact(state: str, delivery_ratio: float) -> dict[str, object]:
        return {
            "result": {
                "state": state,
                "failedReceptionMetricsComplete": True,
                "submitted": 1,
                "delivered": 1,
                "metrics": {"receiverDeliveryRatio": delivery_ratio},
            },
            "elapsedSeconds": 1,
            "resources": {},
            "flows": {
                "default": {
                    "requested": 1,
                    "deliveryRatio": delivery_ratio,
                }
            },
        }

    summary = summarize_trials(
        experiment,
        [
            artifact("COMPLETED", 0.5),  # type: ignore[arg-type]
            artifact("CANCELLED", 1.0),  # type: ignore[arg-type]
            artifact("FAILED", 1.0),  # type: ignore[arg-type]
        ],
    )

    assert summary["completedTrials"] == 1
    assert summary["incompleteTrials"] == 2
    assert summary["terminalStateCounts"] == {
        "CANCELLED": 1,
        "COMPLETED": 1,
        "FAILED": 1,
    }
    assert summary["receiverDeliveryRatio"] == {
        "median": 0.5,
        "minimum": 0.5,
        "maximum": 0.5,
    }
    assert summary["flows"]["default"]["deliveryRatio"] == {
        "median": 0.5,
        "minimum": 0.5,
        "maximum": 0.5,
    }


def test_fresh_trials_restart_native_firmware_for_each_repetition(
    monkeypatch, tmp_path: Path
) -> None:
    starts = []

    async def fake_start_scenario(client, definition):
        starts.append((client, definition))
        return {
            "scenario": definition["name"],
            "startupSeconds": 0.0,
            "state": {"state": "RUNNING"},
            "nodes": [],
        }

    async def fake_run_experiment(client, experiment, output_dir, trial):
        return {"name": f"{experiment.name}-r{trial}"}

    monkeypatch.setattr(system_bench, "start_scenario", fake_start_scenario)
    monkeypatch.setattr(system_bench, "run_experiment", fake_run_experiment)
    definition = {"name": "fresh-trial-test"}
    experiment = Experiment("repeated", {}, repetitions=3)

    startups, artifacts = asyncio.run(
        system_bench.run_fresh_trials(object(), definition, experiment, tmp_path)
    )

    assert len(starts) == 3
    assert [startup["trial"] for startup in startups] == [1, 2, 3]
    assert [artifact["name"] for artifact in artifacts] == [
        "repeated-r1",
        "repeated-r2",
        "repeated-r3",
    ]

import asyncio
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

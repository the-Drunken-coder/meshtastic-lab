import asyncio
from pathlib import Path

import experiments.run_system_bench as system_bench
from backend.app.traffic import TrafficController
from experiments.run_system_bench import (
    Experiment,
    load_workload,
    summarize_flows,
    summarize_trials,
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

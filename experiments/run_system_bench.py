#!/usr/bin/env python3
"""Run repeatable firmware-in-the-loop capacity experiments through the public API."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import time
from collections.abc import Awaitable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.app.metrics import percentile
from backend.app.models import RFSettings, TopologyPreset, apply_topology_preset, default_scenario
from backend.app.traffic import MAX_DRAIN_SECONDS, SourceTiming, TrafficRunRequest

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "data" / "experiments"
BASE_URL = "http://127.0.0.1:8080"
CONTAINER = "meshtastic-lab-meshtastic-lab-1"
TERMINAL_TRAFFIC_STATES = {"COMPLETED", "FAILED", "CANCELLED"}
TRAFFIC_WAIT_GRACE_SECONDS = 30.0


@dataclass(frozen=True)
class ResourceSample:
    monotonic_seconds: float
    cpu_percent: float
    memory: str
    pids: int


@dataclass(frozen=True)
class Experiment:
    name: str
    request: dict[str, Any]
    link_schedule: tuple[tuple[float, dict[str, Any]], ...] = ()
    repetitions: int = 1


class WorkloadScenario(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_count: Annotated[int, Field(ge=2, le=10)] = Field(alias="nodeCount")
    topology: TopologyPreset = TopologyPreset.FULL_MESH
    region: str = "US"
    frequency_slot: Annotated[int, Field(ge=0, le=255)] = Field(default=20, alias="frequencySlot")
    hop_limit: Annotated[int, Field(ge=1, le=7)] = Field(default=4, alias="hopLimit")
    fresh_state: bool = Field(default=True, alias="freshState")


class WorkloadDefinition(BaseModel):
    """Declarative preset sweep through one validated traffic request."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    name: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")]
    scenario: WorkloadScenario
    modem_presets: list[str] = Field(alias="modemPresets", min_length=1, max_length=9)
    traffic: TrafficRunRequest
    trials: Annotated[int, Field(ge=1, le=10)] = 1

    @field_validator("modem_presets")
    @classmethod
    def validate_modem_presets(cls, values: list[str]) -> list[str]:
        if len(set(values)) != len(values):
            raise ValueError("modemPresets must be unique")
        for value in values:
            RFSettings(modemPreset=value)
        return values


def scenario(node_count: int, topology: TopologyPreset, modem_preset: str) -> dict[str, Any]:
    value = apply_topology_preset(default_scenario(node_count), topology)
    value.name = f"bench-{node_count}-{topology.value}-{modem_preset.lower()}"
    value.rf.modem_preset = modem_preset
    return value.model_dump(mode="json", by_alias=True)


def workload_scenario(workload: WorkloadDefinition, modem_preset: str) -> dict[str, Any]:
    settings = workload.scenario
    value = apply_topology_preset(default_scenario(settings.node_count), settings.topology)
    value.name = f"{workload.name}-{modem_preset.lower().replace('_', '-')}"
    value.rf = RFSettings(
        region=settings.region,
        modemPreset=modem_preset,
        frequencySlot=settings.frequency_slot,
        hopLimit=settings.hop_limit,
    )
    value.fresh_state = settings.fresh_state
    return value.model_dump(mode="json", by_alias=True)


def load_workload(path: Path) -> WorkloadDefinition:
    return WorkloadDefinition.model_validate_json(path.read_text(encoding="utf-8"))


async def wait_for_state(client: httpx.AsyncClient, expected: str, deadline_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = await client.get("/api/state")
        response.raise_for_status()
        last = response.json()
        if last.get("state") == expected:
            return last
        if last.get("state") == "FAILED":
            raise RuntimeError(f"simulation failed while waiting for {expected}: {last}")
        await asyncio.sleep(0.5)
    raise TimeoutError(f"timed out waiting for {expected}: {last}")


async def sample_resources(samples: list[ResourceSample], stop: asyncio.Event, started: float) -> None:
    while not stop.is_set():
        completed = await asyncio.to_thread(
            subprocess.run,
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.CPUPerc}}|{{.MemUsage}}|{{.PIDs}}",
                CONTAINER,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        fields = completed.stdout.strip().split("|")
        if completed.returncode == 0 and len(fields) == 3:
            samples.append(
                ResourceSample(
                    monotonic_seconds=time.monotonic() - started,
                    cpu_percent=float(fields[0].rstrip("%")),
                    memory=fields[1],
                    pids=int(fields[2]),
                )
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=1)
        except TimeoutError:
            continue


async def timed_resource_capture[ResultT](
    operation: Awaitable[ResultT],
) -> tuple[ResultT, list[ResourceSample], float]:
    samples: list[ResourceSample] = []
    stop = asyncio.Event()
    started = time.monotonic()
    sampler = asyncio.create_task(sample_resources(samples, stop, started))
    try:
        result = await operation
        return result, samples, time.monotonic() - started
    finally:
        stop.set()
        await sampler


async def start_scenario(client: httpx.AsyncClient, definition: dict[str, Any]) -> dict[str, Any]:
    await client.post("/api/simulation/stop")
    await wait_for_state(client, "STOPPED", 30)
    replaced = await client.put("/api/scenario", json=definition)
    replaced.raise_for_status()

    async def start_and_wait() -> dict[str, Any]:
        started = await client.post("/api/simulation/start")
        started.raise_for_status()
        return await wait_for_state(client, "RUNNING", 240)

    state, samples, elapsed = await timed_resource_capture(start_and_wait())
    nodes = (await client.get("/api/nodes")).json()
    return {
        "scenario": definition["name"],
        "startupSeconds": elapsed,
        "state": state,
        "nodes": nodes,
        "resources": summarize_resources(samples),
    }


async def run_fresh_trials(
    client: httpx.AsyncClient,
    definition: dict[str, Any],
    experiment: Experiment,
    output_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Restart native firmware before every repetition."""

    startups = []
    artifacts = []
    for trial in range(1, experiment.repetitions + 1):
        startup = await start_scenario(client, definition)
        startup["experiment"] = experiment.name
        startup["trial"] = trial
        startups.append(startup)
        print(
            f"started {definition['name']} trial {trial} in "
            f"{startup['startupSeconds']:.1f}s; nodes={len(startup['nodes'])}"
        )
        artifacts.append(await run_experiment(client, experiment, output_dir, trial))
    return startups, artifacts


def summarize_resources(samples: list[ResourceSample]) -> dict[str, Any]:
    if not samples:
        return {"samples": 0}
    return {
        "samples": len(samples),
        "peakCpuPercent": max(sample.cpu_percent for sample in samples),
        "peakPids": max(sample.pids for sample in samples),
        "lastMemory": samples[-1].memory,
        "raw": [asdict(sample) for sample in samples],
    }


async def apply_link_schedule(
    client: httpx.AsyncClient,
    schedule: tuple[tuple[float, dict[str, Any]], ...],
    started: float,
) -> None:
    for offset, link in schedule:
        await asyncio.sleep(max(0, started + offset - time.monotonic()))
        response = await client.put("/api/links", json=link)
        response.raise_for_status()


async def wait_for_traffic(
    client: httpx.AsyncClient,
    deadline_seconds: float,
    samples: list[dict[str, Any]],
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = time.monotonic() + deadline_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = await client.get("/api/traffic/runs/current")
        response.raise_for_status()
        last = response.json()
        drain_remaining = last.get("drainDeadlineSecondsRemaining")
        sample = {
            "elapsedSeconds": round(time.monotonic() - started, 3),
            "state": last.get("state"),
            "phase": last.get("phase"),
            "requested": last.get("requested"),
            "submitted": last.get("submitted"),
            "pendingFirmwareAdmissions": last.get("pendingFirmwareAdmissions"),
            "unresolvedDirectMessages": last.get("unresolvedDirectMessages"),
            "drainDeadlineSecondsRemaining": (
                round(float(drain_remaining), 1) if drain_remaining is not None else None
            ),
        }
        samples.append(sample)
        if last.get("state") in TERMINAL_TRAFFIC_STATES:
            return last
        await asyncio.sleep(0.5)
    raise TimeoutError(f"traffic did not finish: {last}")


async def export_result(client: httpx.AsyncClient, run_id: str) -> dict[str, Any]:
    """Terminal status guarantees that the immutable export is already available."""

    response = await client.get(f"/api/traffic/runs/{run_id}/export")
    response.raise_for_status()
    return cast(dict[str, Any], response.json())


def traffic_wait_deadline(request: dict[str, Any]) -> float:
    validated = TrafficRunRequest.model_validate(request)
    maximum_phase_offset = max(
        (
            0.0
            if flow.source_timing == SourceTiming.ALIGNED
            else 60 / flow.messages_per_minute
        )
        for flow in validated.scheduling_flows()
    )
    return (
        validated.duration_seconds
        + maximum_phase_offset
        + MAX_DRAIN_SECONDS
        + TRAFFIC_WAIT_GRACE_SECONDS
    )


def summarize_flows(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    request = cast(dict[str, Any], result["request"])
    acknowledgments_expected = (
        request.get("kind") == "direct-text"
        and bool(request.get("acknowledgmentRequested"))
    )
    configured = request.get("flows") or [
        {
            "name": "default",
            "sourceNodes": request["sourceNodes"],
            "messagesPerMinute": request["messagesPerMinute"],
            "destinationStrategy": request["destinationStrategy"],
            "fixedDestination": request.get("fixedDestination"),
            "sourceTiming": request.get("sourceTiming", "aligned"),
        }
    ]
    definitions = {flow["name"]: flow for flow in configured}
    generated = cast(list[dict[str, Any]], result["generatedMessages"])
    summaries: dict[str, dict[str, Any]] = {}
    for name, definition in definitions.items():
        messages = [message for message in generated if message.get("flow", "default") == name]
        submitted = [message for message in messages if message["submitted"]]
        delivered = [
            message
            for message in messages
            if (
                any(receiver != message["sourceNode"] for receiver in message["deliveredTo"])
                if message["destinationNode"] == "broadcast"
                else message["destinationNode"] in message["deliveredTo"]
            )
        ]
        latencies = [
            float(message["latencyMs"]) for message in delivered if message.get("latencyMs") is not None
        ]
        requested_count = len(messages)
        submitted_count = len(submitted)
        interval_seconds = 60 / float(definition["messagesPerMinute"])
        on_time_deliveries = sum(latency <= interval_seconds * 1000 for latency in latencies)
        summaries[name] = {
            "sourceNodes": definition["sourceNodes"],
            "sourceCount": len(definition["sourceNodes"]),
            "messagesPerMinutePerSource": definition["messagesPerMinute"],
            "intervalSeconds": interval_seconds,
            "destinationStrategy": definition["destinationStrategy"],
            "fixedDestination": definition.get("fixedDestination"),
            "sourceTiming": definition.get("sourceTiming", "aligned"),
            "requested": requested_count,
            "submitted": submitted_count,
            "submissionFailed": sum(message.get("submissionError") is not None for message in messages),
            "transmitted": sum(bool(message["transmitted"]) for message in messages),
            "delivered": len(delivered),
            "acknowledged": sum(bool(message["acknowledged"]) for message in messages),
            "admissionRatio": submitted_count / requested_count if requested_count else None,
            "deliveryRatio": len(delivered) / requested_count if requested_count else None,
            "onTimeDeliveries": on_time_deliveries,
            "onTimeDeliveryRatio": (on_time_deliveries / requested_count if requested_count else None),
            "acknowledgmentSuccessRatio": (
                sum(bool(message["acknowledged"]) for message in submitted) / submitted_count
                if acknowledgments_expected and submitted_count
                else None
            ),
            "medianLatencyMs": percentile(latencies, 0.5),
            "p95LatencyMs": percentile(latencies, 0.95, minimum_samples=20),
            "maximumLatencyMs": max(latencies, default=None),
        }
    return summaries


async def run_experiment(
    client: httpx.AsyncClient, experiment: Experiment, output_dir: Path, trial: int
) -> dict[str, Any]:
    response = await client.post("/api/traffic/runs", json=experiment.request)
    response.raise_for_status()
    run_id = response.json()["runId"]
    started = time.monotonic()
    link_task = asyncio.create_task(apply_link_schedule(client, experiment.link_schedule, started))
    status_samples: list[dict[str, Any]] = []
    try:
        _, samples, elapsed = await timed_resource_capture(
            wait_for_traffic(client, traffic_wait_deadline(experiment.request), status_samples)
        )
        await link_task
    except BaseException:
        link_task.cancel()
        await asyncio.gather(link_task, return_exceptions=True)
        raise
    exported = await export_result(client, run_id)
    flows = summarize_flows(exported)
    artifact_name = experiment.name if experiment.repetitions == 1 else f"{experiment.name}-r{trial}"
    artifact = {
        "name": artifact_name,
        "experiment": experiment.name,
        "trial": trial,
        "elapsedSeconds": elapsed,
        "resources": summarize_resources(samples),
        "trafficStatusSamples": status_samples,
        "result": exported,
        "flows": flows,
    }
    (output_dir / f"{artifact_name}.json").write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    metrics = exported["metrics"]
    print(
        f"{artifact_name}: state={exported['state']} "
        f"requested={exported['requested']} submitted={exported['submitted']} "
        f"delivered={exported['delivered']} receiverRatio={metrics['receiverDeliveryRatio']} "
        f"rfTx={metrics['rfTransmissions']} failedRx={metrics['failedReceptions']} "
        f"elapsed={elapsed:.1f}s"
    )
    for name, flow in flows.items():
        print(
            f"  {name}: requested={flow['requested']} submitted={flow['submitted']} "
            f"delivered={flow['delivered']} acked={flow['acknowledged']} "
            f"p95={flow['p95LatencyMs']}"
        )
    return artifact


def summarize_trials(experiment: Experiment, artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    def values(path: tuple[str, ...]) -> list[float]:
        selected: list[float] = []
        for artifact in artifacts:
            value: Any = artifact
            for key in path:
                if not isinstance(value, dict) or key not in value:
                    value = None
                    break
                value = value[key]
            if value is not None:
                selected.append(float(value))
        return selected

    def distribution(path: tuple[str, ...]) -> dict[str, float] | None:
        selected = values(path)
        if not selected:
            return None
        return {
            "median": statistics.median(selected),
            "minimum": min(selected),
            "maximum": max(selected),
        }

    flow_timings = {flow.get("sourceTiming", "aligned") for flow in experiment.request.get("flows", [])}
    source_timing = (
        next(iter(flow_timings))
        if len(flow_timings) == 1
        else "mixed"
        if flow_timings
        else experiment.request.get("sourceTiming", "aligned")
    )

    return {
        "name": experiment.name,
        "trials": len(artifacts),
        "failedReceptionMetricsCompleteTrials": sum(
            bool(artifact["result"]["failedReceptionMetricsComplete"]) for artifact in artifacts
        ),
        "sourceTiming": source_timing,
        "submitted": distribution(("result", "submitted")),
        "uniqueDelivered": distribution(("result", "delivered")),
        "receiverDeliveryRatio": distribution(("result", "metrics", "receiverDeliveryRatio")),
        "medianLatencyMs": distribution(("result", "metrics", "medianLatencyMs")),
        "p95LatencyMs": distribution(("result", "metrics", "p95LatencyMs")),
        "rfTransmissions": distribution(("result", "metrics", "rfTransmissions")),
        "observedAirtimeMs": distribution(("result", "metrics", "observedAirtimeMs")),
        "elapsedSeconds": distribution(("elapsedSeconds",)),
        "peakCpuPercent": distribution(("resources", "peakCpuPercent")),
        "flows": {
            name: {
                "requested": distribution(("flows", name, "requested")),
                "admissionRatio": distribution(("flows", name, "admissionRatio")),
                "deliveryRatio": distribution(("flows", name, "deliveryRatio")),
                "onTimeDeliveryRatio": distribution(("flows", name, "onTimeDeliveryRatio")),
                "acknowledgmentSuccessRatio": distribution(("flows", name, "acknowledgmentSuccessRatio")),
                "medianLatencyMs": distribution(("flows", name, "medianLatencyMs")),
                "p95LatencyMs": distribution(("flows", name, "p95LatencyMs")),
                "maximumLatencyMs": distribution(("flows", name, "maximumLatencyMs")),
            }
            for name in sorted(
                {name for artifact in artifacts for name in cast(dict[str, Any], artifact["flows"])}
            )
        },
    }


async def main(
    *,
    only: set[str] | None = None,
    trials: int | None = None,
    workload_path: Path | None = None,
) -> None:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = OUTPUT_ROOT / timestamp
    output_dir.mkdir(parents=True)
    sources_10 = [f"node-{index}" for index in range(1, 11)]

    def link(source: str, target: str, enabled: bool) -> dict[str, Any]:
        return {
            "from": source,
            "to": target,
            "enabled": enabled,
            "rssiDbm": -85,
            "snrDb": 8,
        }

    groups = [
        (
            scenario(10, TopologyPreset.FULL_MESH, "LONG_FAST"),
            [
                Experiment(
                    "long-fast-one-talker",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": ["node-1"],
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 6,
                        "payloadBytes": 64,
                        "durationSeconds": 21,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "aligned",
                        "seed": 101,
                    },
                    repetitions=3,
                ),
                Experiment(
                    "long-fast-ten-talker-aligned",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": sources_10,
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 3,
                        "payloadBytes": 64,
                        "durationSeconds": 21,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "aligned",
                        "seed": 102,
                    },
                    repetitions=3,
                ),
                Experiment(
                    "long-fast-ten-talker-jittered",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": sources_10,
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 3,
                        "payloadBytes": 64,
                        "durationSeconds": 21,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "deterministic-jitter",
                        "seed": 102,
                    },
                    repetitions=3,
                ),
                Experiment(
                    "long-fast-ingest-saturation",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": sources_10,
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 600,
                        "payloadBytes": 200,
                        "durationSeconds": 10,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "aligned",
                        "seed": 103,
                    },
                ),
            ],
        ),
        (
            scenario(10, TopologyPreset.FULL_MESH, "SHORT_FAST"),
            [
                Experiment(
                    "short-fast-ten-talker-aligned",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": sources_10,
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 3,
                        "payloadBytes": 64,
                        "durationSeconds": 21,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "aligned",
                        "seed": 102,
                    },
                    repetitions=3,
                ),
                Experiment(
                    "short-fast-ten-talker-jittered",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": sources_10,
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 3,
                        "payloadBytes": 64,
                        "durationSeconds": 21,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "deterministic-jitter",
                        "seed": 102,
                    },
                    repetitions=3,
                ),
            ],
        ),
        (
            scenario(5, TopologyPreset.LINE, "LONG_FAST"),
            [
                Experiment(
                    "long-fast-four-hop-direct",
                    {
                        "kind": "direct-text",
                        "sourceNodes": ["node-1"],
                        "destinationStrategy": "fixed",
                        "fixedDestination": "node-5",
                        "messagesPerMinute": 3,
                        "payloadBytes": 64,
                        "durationSeconds": 21,
                        "acknowledgmentRequested": True,
                        "sourceTiming": "aligned",
                        "seed": 104,
                    },
                ),
                Experiment(
                    "long-fast-partition-recovery",
                    {
                        "kind": "direct-text",
                        "sourceNodes": ["node-1"],
                        "destinationStrategy": "fixed",
                        "fixedDestination": "node-5",
                        "messagesPerMinute": 3,
                        "payloadBytes": 64,
                        "durationSeconds": 41,
                        "acknowledgmentRequested": True,
                        "sourceTiming": "aligned",
                        "seed": 105,
                    },
                    (
                        (12, link("node-3", "node-4", False)),
                        (12, link("node-4", "node-3", False)),
                        (32, link("node-3", "node-4", True)),
                        (32, link("node-4", "node-3", True)),
                    ),
                ),
            ],
        ),
        (
            json.loads((ROOT / "scenarios" / "hidden-terminal.json").read_text()),
            [
                Experiment(
                    "long-fast-hidden-terminal",
                    {
                        "kind": "broadcast-text",
                        "sourceNodes": ["node-1", "node-3"],
                        "destinationStrategy": "fixed",
                        "messagesPerMinute": 60,
                        "payloadBytes": 200,
                        "durationSeconds": 5,
                        "acknowledgmentRequested": False,
                        "sourceTiming": "aligned",
                        "seed": 106,
                    },
                )
            ],
        ),
    ]
    workload: WorkloadDefinition | None = None
    if workload_path is not None:
        if only is not None:
            raise ValueError("--only cannot be combined with --workload")
        workload = load_workload(workload_path)
        request = workload.traffic.model_dump(mode="json", by_alias=True)
        repetitions = trials or workload.trials
        groups = [
            (
                workload_scenario(workload, preset),
                [
                    Experiment(
                        name=f"{workload.name}-{preset.lower().replace('_', '-')}",
                        request=request,
                        repetitions=repetitions,
                    )
                ],
            )
            for preset in workload.modem_presets
        ]
    else:
        known_names = {experiment.name for _definition, experiments in groups for experiment in experiments}
        unknown_names = (only or set()) - known_names
        if unknown_names:
            raise ValueError(f"unknown experiments: {', '.join(sorted(unknown_names))}")
    if workload is None and (only is not None or trials is not None):
        selected_groups = []
        for definition, experiments in groups:
            selected = [
                replace(experiment, repetitions=trials or experiment.repetitions)
                for experiment in experiments
                if only is None or experiment.name in only
            ]
            if selected:
                selected_groups.append((definition, selected))
        groups = selected_groups

    summary: dict[str, Any] = {
        "startedAt": datetime.now(UTC).isoformat(),
        "workload": (workload.model_dump(mode="json", by_alias=True) if workload is not None else None),
        "groups": [],
        "experiments": [],
        "aggregates": [],
    }
    timeout = httpx.Timeout(450, connect=10)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        summary["capabilities"] = (await client.get("/api/capabilities")).json()
        try:
            for definition, experiments in groups:
                for experiment in experiments:
                    startups, artifacts = await run_fresh_trials(
                        client,
                        definition,
                        experiment,
                        output_dir,
                    )
                    summary["groups"].extend(startups)
                    for trial, artifact in enumerate(artifacts, start=1):
                        summary["experiments"].append(
                            {
                                "name": artifact["name"],
                                "experiment": experiment.name,
                                "trial": trial,
                                "artifact": f"{artifact['name']}.json",
                                "elapsedSeconds": artifact["elapsedSeconds"],
                                "resources": artifact["resources"],
                                "flows": artifact["flows"],
                                "result": {
                                    key: artifact["result"][key]
                                    for key in (
                                        "runId",
                                        "state",
                                        "requested",
                                        "submitted",
                                        "submissionFailed",
                                        "transmitted",
                                        "delivered",
                                        "metrics",
                                        "failure",
                                    )
                                },
                            }
                        )
                    summary["aggregates"].append(summarize_trials(experiment, artifacts))
        finally:
            await client.post("/api/simulation/stop")
            await wait_for_state(client, "STOPPED", 30)

    summary["finishedAt"] = datetime.now(UTC).isoformat()
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"results: {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only",
        action="append",
        help="run only this named experiment; repeat the flag to select more than one",
    )
    parser.add_argument(
        "--trials",
        type=int,
        choices=range(1, 11),
        metavar="1-10",
        help="override the configured repetition count for selected experiments",
    )
    parser.add_argument(
        "--workload",
        type=Path,
        help="run every modem preset in a declarative workload JSON file",
    )
    arguments = parser.parse_args()
    asyncio.run(
        main(
            only=set(arguments.only) if arguments.only else None,
            trials=arguments.trials,
            workload_path=arguments.workload,
        )
    )

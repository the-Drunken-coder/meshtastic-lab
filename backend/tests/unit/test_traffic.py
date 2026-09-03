from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path

import pytest
from meshtastic.protobuf import mesh_pb2, portnums_pb2

from backend.app.metrics import EventBroker, EventType
from backend.app.models import Scenario, default_scenario
from backend.app.traffic import (
    DestinationStrategy,
    FailedReceptionSample,
    TrafficController,
    TrafficKind,
    TrafficRunRequest,
    TrafficRunResult,
    TrafficRunState,
    TrafficRunSummary,
    summarize_result,
)


class FakeGateway:
    def __init__(self, node_id: str = "", *, queue_result: int = 0) -> None:
        self.node_id = node_id
        self.queue_result = queue_result
        self.controller: TrafficController | None = None
        self.sent: list[mesh_pb2.ToRadio] = []
        self.sent_event = asyncio.Event()

    async def send_to_radio(self, message: mesh_pb2.ToRadio, *, source: str = "controller") -> None:
        del source
        copy = mesh_pb2.ToRadio()
        copy.CopyFrom(message)
        self.sent.append(copy)
        self.sent_event.set()
        if self.controller is not None and message.WhichOneof("payload_variant") == "packet":
            status = mesh_pb2.FromRadio()
            status.queueStatus.res = self.queue_result
            status.queueStatus.mesh_packet_id = message.packet.id
            await self.controller.handle_from_radio(self.node_id, status)


@pytest.mark.asyncio
async def test_deterministic_traffic_schedule_and_persistence(tmp_path: Path) -> None:
    scenario = default_scenario(3)
    gateways = {node.id: FakeGateway(node.id) for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2, "node-3": 3},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    request = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1"],
        destinationStrategy=DestinationStrategy.DETERMINISTIC_RANDOM,
        messagesPerMinute=600,
        payloadBytes=64,
        durationSeconds=0.21,
        acknowledgmentRequested=True,
        seed=7,
    )

    run_id = controller.start(request)
    result = await controller.wait(deadline_seconds=2)

    assert result.run_id == run_id
    assert result.state == TrafficRunState.COMPLETED
    assert result.requested == 3
    assert result.submitted == 3
    assert [message.packet_id for message in result.generated_messages] == [
        253970985,
        3250102041,
        108967520,
    ]
    assert (tmp_path / f"{run_id}.json").is_file()
    assert (tmp_path / f"{run_id}.summary.json").is_file()
    assert json.loads((tmp_path / f"{run_id}.json").read_text())["schemaVersion"] == 1


@pytest.mark.asyncio
async def test_stop_waits_for_terminal_persistence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller(tmp_path)
    persistence_started = threading.Event()
    release_persistence = threading.Event()
    persist_calls = 0
    persist = controller._freeze_and_persist

    def blocking_persist(current: TrafficRunResult) -> TrafficRunResult:
        nonlocal persist_calls
        persist_calls += 1
        persistence_started.set()
        if not release_persistence.wait(timeout=2):
            raise RuntimeError("test did not release persistence")
        return persist(current)

    monkeypatch.setattr(controller, "_freeze_and_persist", blocking_persist)
    controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        )
    )
    assert await asyncio.to_thread(persistence_started.wait, 1)

    stop_task = asyncio.create_task(controller.stop())
    await asyncio.sleep(0)
    try:
        assert not stop_task.done()
    finally:
        release_persistence.set()
    await stop_task

    result = controller.result()
    assert result is not None
    assert result.state == TrafficRunState.COMPLETED
    assert persist_calls == 1


@pytest.mark.asyncio
async def test_stop_bounds_wait_for_terminal_persistence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller(tmp_path, finalization_wait_seconds=0.01)
    persistence_started = threading.Event()
    release_persistence = threading.Event()
    persist = controller._freeze_and_persist

    def blocking_persist(current: TrafficRunResult) -> TrafficRunResult:
        persistence_started.set()
        if not release_persistence.wait(timeout=2):
            raise RuntimeError("test did not release persistence")
        return persist(current)

    monkeypatch.setattr(controller, "_freeze_and_persist", blocking_persist)
    controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        )
    )
    assert await asyncio.to_thread(persistence_started.wait, 1)

    started = time.monotonic()
    try:
        assert await controller.stop() is False
        assert time.monotonic() - started < 0.5
        assert not controller._finalization_done.is_set()
    finally:
        release_persistence.set()

    result = await controller.wait(deadline_seconds=2)
    assert result.state == TrafficRunState.COMPLETED
    assert controller.result_is_finalized(result.run_id)


def test_payload_is_validated_after_identifier_encoding(tmp_path: Path) -> None:
    scenario = default_scenario(2)
    controller = TrafficController(
        scenario=scenario,
        gateways={"node-1": FakeGateway(), "node-2": FakeGateway()},  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
    )
    request = TrafficRunRequest(
        kind=TrafficKind.BROADCAST_TEXT,
        sourceNodes=["node-1"],
        payloadBytes=16,
        durationSeconds=1,
    )

    with pytest.raises(ValueError, match="encoded traffic identifier"):
        controller.start(request)


def _controller(
    tmp_path: Path,
    scenario: Scenario | None = None,
    *,
    finalization_wait_seconds: float = 5.0,
) -> TrafficController:
    selected = default_scenario(3) if scenario is None else scenario
    gateways = {node.id: FakeGateway(node.id) for node in selected.nodes}
    controller = TrafficController(
        scenario=selected,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={node.id: index for index, node in enumerate(selected.nodes, start=1)},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
        finalization_wait_seconds=finalization_wait_seconds,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    return controller


def _text_from_radio(
    *, run_id: str, sequence: int, packet_id: int, origin: int
) -> mesh_pb2.FromRadio:
    message = mesh_pb2.FromRadio()
    message.packet.id = packet_id
    setattr(message.packet, "from", origin)
    message.packet.decoded.portnum = portnums_pb2.TEXT_MESSAGE_APP
    message.packet.decoded.payload = f"ML1:{run_id}:{sequence}:payload".encode()
    return message


def _rf_packet(
    *, run_id: str, sequence: int, packet_id: int, origin: int
) -> mesh_pb2.MeshPacket:
    compressed = mesh_pb2.Compressed(
        portnum=portnums_pb2.TEXT_MESSAGE_APP,
        data=f"ML1:{run_id}:{sequence}:payload".encode(),
    )
    packet = mesh_pb2.MeshPacket(id=packet_id)
    setattr(packet, "from", origin)
    packet.decoded.portnum = portnums_pb2.SIMULATOR_APP
    packet.decoded.payload = compressed.SerializeToString()
    return packet


def _routing_rf_packet(
    *, request_id: int, packet_id: int, origin: int, destination: int
) -> mesh_pb2.MeshPacket:
    routing = mesh_pb2.Routing(error_reason=mesh_pb2.Routing.Error.NONE)
    compressed = mesh_pb2.Compressed(
        portnum=portnums_pb2.ROUTING_APP,
        data=routing.SerializeToString(),
    )
    packet = mesh_pb2.MeshPacket(id=packet_id, to=destination)
    setattr(packet, "from", origin)
    packet.decoded.portnum = portnums_pb2.SIMULATOR_APP
    packet.decoded.request_id = request_id
    packet.decoded.payload = compressed.SerializeToString()
    return packet


@pytest.mark.asyncio
async def test_live_summary_excludes_unbounded_generated_records(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=10, payloadBytes=64
    )
    controller.start(request)
    await asyncio.sleep(0)
    assert controller.current is not None
    controller.current.generated_messages *= 100_000
    assert controller.summary() is not None
    summary = controller.summary()
    assert summary is not None
    dumped = summary.model_dump(by_alias=True)
    assert "generatedMessages" not in dumped
    assert "receiversPerBroadcast" not in dumped["metrics"]
    assert len(summary.model_dump_json(by_alias=True)) < 20_000
    controller.current.generated_messages.clear()
    await controller.stop()
    result = controller.result()
    assert controller.state == TrafficRunState.CANCELLED
    assert result is not None and result.state == TrafficRunState.CANCELLED


@pytest.mark.asyncio
async def test_late_events_and_foreign_drops_do_not_mutate_frozen_result(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=600, durationSeconds=0.01, payloadBytes=64
    )
    run_id = controller.start(request)
    result = await controller.wait(deadline_seconds=2)
    assert result.state == TrafficRunState.COMPLETED
    generated = result.generated_messages[0]
    packet = mesh_pb2.MeshPacket(id=generated.packet_id)
    setattr(packet, "from", 1)
    packet.decoded.portnum = 1
    packet.decoded.payload = f"ML1:{run_id}:1:".encode()
    controller.record_drop("node-1", mesh_pb2.MeshPacket(id=123), "background")
    late = mesh_pb2.FromRadio()
    late.packet.CopyFrom(packet)
    await controller.handle_from_radio("node-2", late)
    assert controller.result() == result
    persisted = json.loads((tmp_path / f"{run_id}.json").read_text())
    assert persisted == result.model_dump(mode="json", by_alias=True)


@pytest.mark.asyncio
async def test_packet_ids_are_reserved_per_source(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1)
    controller.start(request)
    class DuplicateRandom:
        def __init__(self) -> None:
            self.values = iter([7, 7, 8])

        def randrange(self, _start: int, _stop: int) -> int:
            return next(self.values)

    randomizer = DuplicateRandom()
    first = controller._allocate_packet_id("node-1", randomizer)  # type: ignore[arg-type]
    second = controller._allocate_packet_id("node-1", randomizer)  # type: ignore[arg-type]
    assert (first, second) == (7, 8)
    await controller.stop()


@pytest.mark.asyncio
async def test_same_packet_id_from_different_sources_keeps_ack_correlation(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1", "node-2"],
        fixedDestination="node-3",
        messagesPerMinute=0.1,
        durationSeconds=1,
        payloadBytes=64,
    )
    controller.start(request)

    class SameIdRandom:
        def randrange(self, _start: int, _stop: int) -> int:
            return 7

    randomizer = SameIdRandom()
    await controller._submit("node-1", "node-3", randomizer)  # type: ignore[arg-type]
    await controller._submit("node-2", "node-3", randomizer)  # type: ignore[arg-type]
    assert controller.current is not None
    first, second = controller.current.generated_messages
    assert first.packet_id == second.packet_id == 7

    controller.record_rf_transmission(
        "node-1",
        _rf_packet(run_id=controller.current.run_id, sequence=first.sequence, packet_id=7, origin=1),
        10,
    )

    acknowledgment = mesh_pb2.FromRadio()
    acknowledgment.packet.id = 91
    setattr(acknowledgment.packet, "from", 3)
    acknowledgment.packet.decoded.portnum = portnums_pb2.ROUTING_APP
    acknowledgment.packet.decoded.request_id = 7
    acknowledgment.packet.decoded.payload = mesh_pb2.Routing(
        error_reason=mesh_pb2.Routing.Error.NONE
    ).SerializeToString()
    await controller.handle_from_radio("node-1", acknowledgment)

    assert first.acknowledged
    assert not second.acknowledged
    acknowledgment_event = next(
        event
        for event in controller.event_broker.recent()
        if event.event_type == EventType.ACKNOWLEDGMENT
    )
    assert acknowledgment_event.mesh_packet_id == 91
    assert acknowledgment_event.traffic_sequence == first.sequence
    await controller.stop()


@pytest.mark.asyncio
async def test_rf_and_drop_correlation_supports_encrypted_native_payload(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1, payloadBytes=64
    )
    controller.start(request)

    class FixedRandom:
        def randrange(self, _start: int, _stop: int) -> int:
            return 7

    await controller._submit("node-1", "broadcast", FixedRandom())  # type: ignore[arg-type]
    foreign = mesh_pb2.MeshPacket(id=8)
    setattr(foreign, "from", 1)
    controller.record_rf_transmission("node-1", foreign, 10)
    controller.record_drop("node-1", foreign, "link-disabled")

    summary = controller.summary()
    assert summary is not None
    assert summary.transmitted == 0
    assert summary.metrics.drops_by_reason == {}

    encrypted = mesh_pb2.Compressed(
        portnum=portnums_pb2.UNKNOWN_APP,
        data=b"encrypted-native-payload",
    )
    legitimate = mesh_pb2.MeshPacket(id=7)
    setattr(legitimate, "from", 1)
    legitimate.decoded.portnum = portnums_pb2.SIMULATOR_APP
    legitimate.decoded.payload = encrypted.SerializeToString()
    controller.record_rf_transmission("node-1", legitimate, 10)
    controller.record_drop("node-1", legitimate, "link-disabled")
    summary = controller.summary()
    assert summary is not None
    assert summary.transmitted == 1
    assert summary.metrics.drops_by_reason == {"link-disabled": 1}
    await controller.stop()


@pytest.mark.asyncio
async def test_broadcast_delivery_ratio_counts_messages_once(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1, payloadBytes=64
    )
    run_id = controller.start(request)

    class FixedRandom:
        def randrange(self, _start: int, _stop: int) -> int:
            return 7

    await controller._submit("node-1", "broadcast", FixedRandom())  # type: ignore[arg-type]
    received = _text_from_radio(run_id=run_id, sequence=1, packet_id=7, origin=1)
    await controller.handle_from_radio("node-2", received)
    await controller.handle_from_radio("node-3", received)
    live = controller.summary()
    assert live is not None
    assert live.metrics.median_latency_ms is not None
    metric_events = [
        event for event in controller.event_broker.recent() if event.event_type == EventType.METRICS
    ]
    assert any("latestLatencyMs" in event.metric_update for event in metric_events)
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.delivered == 1
    assert result.metrics.unique_application_messages_delivered == 1
    assert result.metrics.delivery_ratio == 1
    assert result.metrics.receiver_deliveries == 2
    assert result.metrics.receivers_per_broadcast == {"1": 2}


@pytest.mark.asyncio
async def test_direct_relay_observation_counts_only_destination_delivery(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1"],
        fixedDestination="node-3",
        messagesPerMinute=0.1,
        durationSeconds=1,
        payloadBytes=64,
    )
    run_id = controller.start(request)

    class FixedRandom:
        def randrange(self, _start: int, _stop: int) -> int:
            return 7

    await controller._submit("node-1", "node-3", FixedRandom())  # type: ignore[arg-type]
    packet = _rf_packet(run_id=run_id, sequence=1, packet_id=7, origin=1)
    controller.record_rf_transmission("node-1", packet, 10)
    controller.record_rf_transmission("node-2", packet, 10)
    received = _text_from_radio(run_id=run_id, sequence=1, packet_id=7, origin=1)
    await controller.handle_from_radio("node-2", received)
    await controller.handle_from_radio("node-3", received)
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.generated_messages[0].delivered_to == ["node-3"]
    assert result.metrics.receiver_deliveries == 1
    assert result.metrics.receiver_delivery_ratio == 1
    assert result.metrics.median_latency_ms is not None
    assert result.metrics.rf_transmissions_per_delivery == 2


@pytest.mark.asyncio
async def test_final_local_stats_are_measured_against_explicit_baseline(tmp_path: Path) -> None:
    async def sample_local_stats() -> dict[str, int]:
        return {"node-1": 12, "node-2": 4, "node-3": 1}

    selected = default_scenario(3)
    controller = TrafficController(
        scenario=selected,
        gateways={node.id: FakeGateway(node.id) for node in selected.nodes},  # type: ignore[arg-type]
        hardware_ids={node.id: index for index, node in enumerate(selected.nodes, start=1)},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
        failed_reception_sampler=sample_local_stats,
    )
    controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        ),
        failed_reception_baseline={"node-1": 10, "node-2": 4, "node-3": 0},
    )

    result = await controller.wait(deadline_seconds=2)

    assert result.state == TrafficRunState.COMPLETED
    assert result.metrics.failed_receptions == 3


@pytest.mark.asyncio
async def test_matching_marker_requires_exact_packet_identity(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1, payloadBytes=64
    )
    run_id = controller.start(request)

    class FixedRandom:
        def randrange(self, _start: int, _stop: int) -> int:
            return 7

    await controller._submit("node-1", "broadcast", FixedRandom())  # type: ignore[arg-type]
    await controller.handle_from_radio(
        "node-2", _text_from_radio(run_id=run_id, sequence=1, packet_id=8, origin=1)
    )
    await controller.handle_from_radio(
        "node-2", _text_from_radio(run_id=run_id, sequence=1, packet_id=7, origin=2)
    )
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.delivered == 0
    assert result.generated_messages[0].delivered_to == []


@pytest.mark.asyncio
async def test_packet_ids_are_not_reused_across_runs(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1, payloadBytes=64
    )

    class SequenceRandom:
        def __init__(self, values: list[int]) -> None:
            self.values = iter(values)

        def randrange(self, _start: int, _stop: int) -> int:
            return next(self.values)

    controller.start(request)
    await controller._submit("node-1", "broadcast", SequenceRandom([7]))  # type: ignore[arg-type]
    await controller.stop()

    controller.start(request)
    await controller._submit("node-1", "broadcast", SequenceRandom([7, 8]))  # type: ignore[arg-type]
    assert controller.current is not None
    assert controller.current.generated_messages[0].packet_id == 8
    await controller.stop()


@pytest.mark.asyncio
async def test_repeat_seed_keeps_destinations_stable_with_bounded_packet_quarantine(
    tmp_path: Path,
) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1"],
        destinationStrategy=DestinationStrategy.DETERMINISTIC_RANDOM,
        messagesPerMinute=600,
        durationSeconds=0.21,
        payloadBytes=64,
        seed=7,
    )

    controller.start(request)
    first = await controller.wait(deadline_seconds=2)
    controller.start(request)
    second = await controller.wait(deadline_seconds=2)

    assert [message.destination_node for message in first.generated_messages] == [
        message.destination_node for message in second.generated_messages
    ]
    retained = sum(len(values) for values in controller._quarantined_packet_ids.values())
    assert retained == len(first.generated_messages)
    assert retained <= 36_000 * len(request.source_nodes)


@pytest.mark.asyncio
async def test_failed_run_cannot_be_downgraded_by_stop(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1, payloadBytes=64
        )
    )
    assert controller.current is not None
    controller.state = TrafficRunState.FAILED
    controller.current.failure = "boom"

    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.state == TrafficRunState.FAILED
    assert result.failure == "boom"


@pytest.mark.asyncio
async def test_persistence_failure_returns_failed_result(tmp_path: Path) -> None:
    results_root = tmp_path / "not-a-directory"
    results_root.write_text("occupied", encoding="utf-8")
    controller = _controller(results_root)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=600, durationSeconds=0.01, payloadBytes=64
    )

    controller.start(request)
    result = await controller.wait(deadline_seconds=2)

    assert controller.state == TrafficRunState.FAILED
    assert result.state == TrafficRunState.FAILED
    assert result.failure is not None and "result persistence failed" in result.failure


@pytest.mark.asyncio
async def test_partial_persistence_failure_removes_completed_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller(tmp_path)
    replace = Path.replace

    def fail_summary_commit(source: Path, target: Path) -> Path:
        if source.name.endswith(".summary.tmp"):
            raise OSError("summary commit failed")
        return replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_summary_commit)
    run_id = controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"], messagesPerMinute=600, durationSeconds=0.01
        )
    )
    result = await controller.wait(deadline_seconds=2)

    assert result.state == TrafficRunState.FAILED
    assert not (tmp_path / f"{run_id}.json").exists()
    assert not (tmp_path / f"{run_id}.summary.json").exists()
    assert not (tmp_path / f"{run_id}.tmp").exists()
    assert not (tmp_path / f"{run_id}.summary.tmp").exists()


@pytest.mark.asyncio
async def test_temporary_write_failure_removes_partial_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller(tmp_path)
    write_text = Path.write_text

    def fail_summary_write(path: Path, data: str, **kwargs: object) -> int:
        if path.name.endswith(".summary.tmp"):
            raise OSError("summary write failed")
        return write_text(path, data, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "write_text", fail_summary_write)
    run_id = controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"], messagesPerMinute=600, durationSeconds=0.01
        )
    )
    result = await controller.wait(deadline_seconds=2)

    assert result.state == TrafficRunState.FAILED
    assert not (tmp_path / f"{run_id}.tmp").exists()
    assert not (tmp_path / f"{run_id}.summary.tmp").exists()


@pytest.mark.asyncio
async def test_legacy_result_provenance_is_migrated_without_invention(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1, payloadBytes=64
    )
    controller.start(request)
    await controller.stop()
    result = controller.result()
    assert result is not None
    legacy = result.model_dump(mode="python", by_alias=True)
    legacy.pop("schemaVersion")
    legacy.pop("collisionPatchSha256")
    legacy.pop("firmwareBinarySha256")
    legacy.pop("buildArchitecture")
    legacy.pop("upstreamBaseImageDigest")
    legacy["firmwareImageDigest"] = "sha256:legacy-upstream"

    migrated = TrafficRunResult.model_validate(legacy)

    assert migrated.schema_version == 1
    assert migrated.collision_patch_sha256 == "unavailable"
    assert migrated.firmware_binary_sha256 == "unavailable"
    assert migrated.build_architecture == "unavailable"
    assert migrated.upstream_base_image_digest == "sha256:legacy-upstream"


@pytest.mark.asyncio
async def test_persisted_results_keep_pre_cap_long_requests_readable(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=0.1,
            durationSeconds=1,
            payloadBytes=64,
        )
    )
    await controller.stop()
    result = controller.result()
    assert result is not None

    result_data = result.model_dump(mode="python", by_alias=True)
    summary_data = summarize_result(result).model_dump(mode="python", by_alias=True)
    for data in (result_data, summary_data):
        data.pop("schemaVersion")
        request_data = data["request"]
        assert isinstance(request_data, dict)
        request_data["messagesPerMinute"] = 600
        request_data["durationSeconds"] = 3600

    loaded_result = TrafficRunResult.model_validate(result_data)
    loaded_summary = TrafficRunSummary.model_validate(summary_data)

    assert loaded_result.request.duration_seconds == 3600
    assert loaded_summary.request.duration_seconds == 3600


@pytest.mark.asyncio
async def test_payload_checks_largest_sequence_and_rejects_source_destination(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    request = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=600, durationSeconds=1, payloadBytes=43
    )
    with pytest.raises(ValueError, match="sequence"):
        controller.start(request)

    floating_boundary = TrafficRunRequest(
        sourceNodes=["node-1"], messagesPerMinute=600, durationSeconds=9.9, payloadBytes=44
    )
    assert controller._maximum_sequence(floating_boundary) == 99
    controller.start(floating_boundary)
    await controller.stop()

    direct = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1", "node-2"],
        fixedDestination="node-1",
        payloadBytes=64,
    )
    with pytest.raises(ValueError, match="one of its source nodes"):
        controller.start(direct)

    stale_destination = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1"],
        destinationStrategy=DestinationStrategy.ROUND_ROBIN,
        fixedDestination="deleted-node",
        payloadBytes=64,
    )
    controller.start(stale_destination)
    await controller.stop()


def test_traffic_start_caps_total_scheduled_messages(tmp_path: Path) -> None:
    request = TrafficRunRequest(
        sourceNodes=["node-1"],
        messagesPerMinute=600,
        durationSeconds=1001,
        payloadBytes=64,
    )

    with pytest.raises(ValueError, match="cannot schedule more than 10000 messages"):
        _controller(tmp_path).start(request)


@pytest.mark.asyncio
async def test_native_duplicate_counter_is_measured_from_explicit_baseline(tmp_path: Path) -> None:
    async def sample_local_stats() -> FailedReceptionSample:
        return FailedReceptionSample(
            totals={"node-1": 0, "node-2": 0, "node-3": 0},
            duplicate_totals={"node-1": 1, "node-2": 5, "node-3": 2},
        )

    selected = default_scenario(3)
    controller = TrafficController(
        scenario=selected,
        gateways={node.id: FakeGateway(node.id) for node in selected.nodes},  # type: ignore[arg-type]
        hardware_ids={node.id: index for index, node in enumerate(selected.nodes, start=1)},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
        failed_reception_sampler=sample_local_stats,
    )
    controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        ),
        failed_reception_baseline=FailedReceptionSample(
            totals={"node-1": 0, "node-2": 0, "node-3": 0},
            duplicate_totals={"node-1": 0, "node-2": 2, "node-3": 2},
        ),
    )

    result = await controller.wait(deadline_seconds=2)

    assert result.metrics.duplicate_receptions == 4


@pytest.mark.asyncio
async def test_routing_ack_rf_frames_are_correlated_with_original_message(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    run_id = controller.start(
        TrafficRunRequest(
            kind=TrafficKind.DIRECT_TEXT,
            sourceNodes=["node-1"],
            fixedDestination="node-3",
            messagesPerMinute=0.1,
            durationSeconds=1,
            payloadBytes=64,
        )
    )

    class FixedRandom:
        def randrange(self, _start: int, _stop: int) -> int:
            return 7

    await controller._submit("node-1", "node-3", FixedRandom())  # type: ignore[arg-type]
    controller.record_rf_transmission(
        "node-1", _rf_packet(run_id=run_id, sequence=1, packet_id=7, origin=1), 10
    )
    acknowledgment = _routing_rf_packet(
        request_id=7,
        packet_id=91,
        origin=3,
        destination=1,
    )
    acknowledgment.decoded.request_id = 0
    acknowledgment.decoded.payload = mesh_pb2.Compressed(
        portnum=portnums_pb2.UNKNOWN_APP,
        data=b"encrypted-routing-payload",
    ).SerializeToString()
    acknowledgment.rx_time = 7
    assert controller.record_rf_transmission("node-3", acknowledgment, 5) == (run_id, 1)
    encrypted_relay = mesh_pb2.MeshPacket()
    encrypted_relay.CopyFrom(acknowledgment)
    encrypted_relay.rx_time = 1_700_000_000
    assert controller.record_rf_transmission("node-2", encrypted_relay, 5) == (run_id, 1)
    controller.record_drop("node-2", encrypted_relay, "link-disabled")
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.transmitted == 1
    assert result.metrics.rf_transmissions == 3
    assert result.metrics.relay_transmissions == 1
    assert result.metrics.observed_airtime_ms == 20
    assert result.metrics.per_node_transmit_counts == {
        "node-1": 1,
        "node-2": 1,
        "node-3": 1,
    }
    assert result.metrics.per_node_airtime_ms == {
        "node-1": 10,
        "node-2": 5,
        "node-3": 5,
    }
    assert result.metrics.drops_by_reason == {"link-disabled": 1}


def test_live_latency_percentiles_use_a_bounded_window(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    latencies = [float(index) for index in range(2058)]
    controller._latencies_ms.extend(latencies)
    controller._live_latencies_ms.extend(latencies)

    metrics = controller._live_metrics_snapshot()

    assert len(controller._latencies_ms) == 2058
    assert len(controller._live_latencies_ms) == 2048
    assert metrics.median_latency_ms == 1033.5


@pytest.mark.asyncio
async def test_terminal_persistence_uses_full_latency_history(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    run_id = controller.start(
        TrafficRunRequest(sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1)
    )
    await controller._cancel_run_task()
    latencies = [float(index) for index in range(2058)]
    controller._latencies_ms.extend(latencies)
    controller._live_latencies_ms.extend([9999.0] * 2048)
    controller._receiver_deliveries = len(latencies)
    controller._receiver_opportunities = len(latencies)

    await controller._finish(TrafficRunState.COMPLETED)

    result = controller.result()
    persisted = json.loads((tmp_path / f"{run_id}.json").read_text(encoding="utf-8"))
    assert result is not None
    assert result.metrics.receiver_deliveries == 2058
    assert result.metrics.median_latency_ms == 1028.5
    assert result.metrics.p95_latency_ms == pytest.approx(1954.15)
    assert result.metrics.p99_latency_ms == pytest.approx(2036.43)
    assert persisted["metrics"]["medianLatencyMs"] == 1028.5
    assert persisted["metrics"]["p99LatencyMs"] == pytest.approx(2036.43)


@pytest.mark.asyncio
async def test_traffic_start_captures_scenario_snapshot(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    snapshot = controller.scenario.model_copy(
        deep=True, update={"rf": controller.scenario.rf.model_copy(update={"hop_limit": 2})}
    )
    request = TrafficRunRequest(sourceNodes=["node-1"], messagesPerMinute=0.1, durationSeconds=1)
    controller.start(request, scenario_snapshot=snapshot)
    assert controller.current is not None
    assert controller.current.scenario_snapshot["rf"]["hopLimit"] == 2
    await controller.stop()


@pytest.mark.asyncio
async def test_firmware_queue_rejection_is_a_submission_failure(tmp_path: Path) -> None:
    scenario = default_scenario(2)
    gateways = {
        node.id: FakeGateway(node.id, queue_result=32) for node in scenario.nodes
    }
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )
    for gateway in gateways.values():
        gateway.controller = controller

    controller.start(
        TrafficRunRequest(
            kind=TrafficKind.DIRECT_TEXT,
            sourceNodes=["node-1"],
            fixedDestination="node-2",
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        )
    )
    result = await controller.wait(deadline_seconds=2)

    assert result.requested == 1
    assert result.submitted == 0
    assert result.submission_failed == 1
    assert result.transmitted == 0
    assert result.generated_messages[0].submitted is False
    assert "res=32" in (result.generated_messages[0].submission_error or "")


@pytest.mark.asyncio
async def test_text_rate_limit_reclassifies_zero_queue_status_as_rejected(
    tmp_path: Path,
) -> None:
    scenario = default_scenario(2)
    gateways = {node.id: FakeGateway(node.id) for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=10,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    controller.start(
        TrafficRunRequest(
            kind=TrafficKind.DIRECT_TEXT,
            sourceNodes=["node-1"],
            fixedDestination="node-2",
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        )
    )
    await gateways["node-1"].sent_event.wait()
    generated = controller.current.generated_messages[0] if controller.current else None
    assert generated is not None and generated.submitted
    assert int(getattr(gateways["node-1"].sent[0].packet, "from")) == 1

    rejection = mesh_pb2.FromRadio()
    setattr(rejection.packet, "from", 1)
    rejection.packet.decoded.portnum = portnums_pb2.ROUTING_APP
    rejection.packet.decoded.request_id = generated.packet_id
    rejection.packet.decoded.payload = mesh_pb2.Routing(
        error_reason=mesh_pb2.Routing.Error.RATE_LIMIT_EXCEEDED
    ).SerializeToString()
    await controller.handle_from_radio("node-1", rejection)
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.requested == 1
    assert result.submitted == 0
    assert result.submission_failed == 1
    assert result.transmitted == 0
    assert "rate limit" in (result.generated_messages[0].submission_error or "")


@pytest.mark.asyncio
async def test_broadcast_acknowledgment_ratio_is_unavailable(tmp_path: Path) -> None:
    controller = _controller(tmp_path)
    controller.start(
        TrafficRunRequest(
            kind=TrafficKind.BROADCAST_TEXT,
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
            acknowledgmentRequested=True,
        )
    )
    result = await controller.wait(deadline_seconds=2)

    gateway = controller.gateways["node-1"]
    assert isinstance(gateway, FakeGateway)
    assert gateway.sent[0].packet.want_ack is False
    assert result.metrics.acknowledgment_success_ratio is None


@pytest.mark.asyncio
async def test_direct_ack_ratio_uses_firmware_accepted_submissions(tmp_path: Path) -> None:
    scenario = default_scenario(3)
    gateways = {
        "node-1": FakeGateway("node-1"),
        "node-2": FakeGateway("node-2", queue_result=32),
        "node-3": FakeGateway("node-3"),
    }
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2, "node-3": 3},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=10,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    run_id = controller.start(
        TrafficRunRequest(
            kind=TrafficKind.DIRECT_TEXT,
            sourceNodes=["node-1", "node-2"],
            fixedDestination="node-3",
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
            acknowledgmentRequested=True,
        )
    )
    await asyncio.gather(
        gateways["node-1"].sent_event.wait(), gateways["node-2"].sent_event.wait()
    )
    assert controller.current is not None
    accepted = next(
        message
        for message in controller.current.generated_messages
        if message.source_node == "node-1"
    )
    controller.record_rf_transmission(
        "node-1",
        _rf_packet(
            run_id=run_id,
            sequence=accepted.sequence,
            packet_id=accepted.packet_id,
            origin=1,
        ),
        10,
    )
    acknowledgment = mesh_pb2.FromRadio()
    setattr(acknowledgment.packet, "from", 3)
    acknowledgment.packet.decoded.portnum = portnums_pb2.ROUTING_APP
    acknowledgment.packet.decoded.request_id = accepted.packet_id
    acknowledgment.packet.decoded.payload = mesh_pb2.Routing(
        error_reason=mesh_pb2.Routing.Error.NONE
    ).SerializeToString()
    await controller.handle_from_radio("node-1", acknowledgment)
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.submitted == 1
    assert result.submission_failed == 1
    assert result.metrics.acknowledgment_success_ratio == 1


@pytest.mark.asyncio
async def test_missing_final_local_stats_do_not_fail_run(tmp_path: Path) -> None:
    async def sample_local_stats() -> FailedReceptionSample:
        return FailedReceptionSample(
            totals={"node-1": 12, "node-3": 1},
            missing_nodes=("node-2",),
        )

    scenario = default_scenario(3)
    gateways = {node.id: FakeGateway(node.id) for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2, "node-3": 3},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
        failed_reception_sampler=sample_local_stats,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    run_id = controller.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        ),
        failed_reception_baseline={"node-1": 10, "node-2": 4, "node-3": 0},
    )

    result = await controller.wait(deadline_seconds=2)

    assert result.state == TrafficRunState.COMPLETED
    assert result.failure is None
    assert result.metrics.failed_receptions == 3
    assert result.failed_reception_metrics_complete is False
    assert result.missing_local_stats_nodes == ["node-2"]
    assert (tmp_path / f"{run_id}.json").is_file()


@pytest.mark.asyncio
async def test_direct_drain_includes_rf_activity_after_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario = default_scenario(2)
    scenario = scenario.model_copy(
        deep=True,
        update={"rf": scenario.rf.model_copy(update={"modem_preset": "LONG_SLOW"})},
    )
    gateways = {node.id: FakeGateway(node.id) for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    monkeypatch.setattr(controller, "_drain_windows", lambda: (0.05, 0.5))
    run_id = controller.start(
        TrafficRunRequest(
            kind=TrafficKind.DIRECT_TEXT,
            sourceNodes=["node-1"],
            fixedDestination="node-2",
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
            acknowledgmentRequested=False,
        )
    )

    async def deliver_then_relay() -> None:
        await gateways["node-1"].sent_event.wait()
        assert controller.current is not None
        generated = controller.current.generated_messages[0]
        await controller.handle_from_radio(
            "node-2",
            _text_from_radio(
                run_id=run_id,
                sequence=generated.sequence,
                packet_id=generated.packet_id,
                origin=1,
            ),
        )
        await asyncio.sleep(0.01)
        controller.record_rf_transmission(
            "node-2",
            _rf_packet(
                run_id=run_id,
                sequence=generated.sequence,
                packet_id=generated.packet_id,
                origin=1,
            ),
            17,
        )

    delivery = asyncio.create_task(deliver_then_relay())
    result = await controller.wait(deadline_seconds=1)
    await delivery

    assert result.state == TrafficRunState.COMPLETED
    assert result.delivered == 1
    assert result.metrics.rf_transmissions == 1
    assert result.metrics.observed_airtime_ms == 17
    assert controller.settle_seconds is None


@pytest.mark.asyncio
async def test_drain_covers_pinned_origin_and_intermediate_retry_budgets(
    tmp_path: Path,
) -> None:
    scenario = default_scenario(2)
    scenario = scenario.model_copy(
        deep=True,
        update={"rf": scenario.rf.model_copy(update={"hop_limit": 1})},
    )
    gateways = {node.id: FakeGateway(node.id) for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )
    for gateway in gateways.values():
        gateway.controller = controller
    controller.start(
        TrafficRunRequest(
            kind=TrafficKind.DIRECT_TEXT,
            sourceNodes=["node-1"],
            fixedDestination="node-2",
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
            acknowledgmentRequested=True,
        )
    )
    await gateways["node-1"].sent_event.wait()

    quiet_seconds, deadline_seconds = controller._drain_windows()
    retransmission_delay_seconds = controller._maximum_retransmission_delay_ms / 1000

    assert quiet_seconds >= retransmission_delay_seconds
    assert deadline_seconds >= retransmission_delay_seconds * 2 * 3 + quiet_seconds
    await controller.stop()

    broadcast_gateways = {
        node.id: FakeGateway(node.id) for node in scenario.nodes
    }
    broadcast = TrafficController(
        scenario=scenario,
        gateways=broadcast_gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )
    for gateway in broadcast_gateways.values():
        gateway.controller = broadcast
    broadcast.start(
        TrafficRunRequest(
            kind=TrafficKind.BROADCAST_TEXT,
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
            acknowledgmentRequested=False,
        )
    )
    await broadcast_gateways["node-1"].sent_event.wait()

    broadcast_quiet, broadcast_deadline = broadcast._drain_windows()
    broadcast_retry_delay = broadcast._maximum_retransmission_delay_ms / 1000

    assert broadcast_deadline >= broadcast_retry_delay * 2 * 2 + broadcast_quiet
    broadcast._maximum_retransmission_delay_ms = 300_000
    assert broadcast._drain_windows()[1] == 300
    await broadcast.stop()

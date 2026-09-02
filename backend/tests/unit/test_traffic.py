from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from meshtastic.protobuf import mesh_pb2, portnums_pb2

from backend.app.metrics import EventBroker
from backend.app.models import Scenario, default_scenario
from backend.app.traffic import (
    DestinationStrategy,
    TrafficController,
    TrafficKind,
    TrafficRunRequest,
    TrafficRunResult,
    TrafficRunState,
)


class FakeGateway:
    def __init__(self) -> None:
        self.sent: list[mesh_pb2.ToRadio] = []

    async def send_to_radio(self, message: mesh_pb2.ToRadio, *, source: str = "controller") -> None:
        del source
        copy = mesh_pb2.ToRadio()
        copy.CopyFrom(message)
        self.sent.append(copy)


@pytest.mark.asyncio
async def test_deterministic_traffic_schedule_and_persistence(tmp_path: Path) -> None:
    scenario = default_scenario(3)
    gateways = {node.id: FakeGateway() for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2, "node-3": 3},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )
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
        4071050725,
        1695753999,
        311111476,
    ]
    assert (tmp_path / f"{run_id}.json").is_file()


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


def _controller(tmp_path: Path, scenario: Scenario | None = None) -> TrafficController:
    selected = default_scenario(3) if scenario is None else scenario
    gateways = {node.id: FakeGateway() for node in selected.nodes}
    return TrafficController(
        scenario=selected,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={node.id: index for index, node in enumerate(selected.nodes, start=1)},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )


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
    setattr(acknowledgment.packet, "from", 3)
    acknowledgment.packet.decoded.portnum = portnums_pb2.ROUTING_APP
    acknowledgment.packet.decoded.request_id = 7
    acknowledgment.packet.decoded.payload = mesh_pb2.Routing(
        error_reason=mesh_pb2.Routing.Error.NONE
    ).SerializeToString()
    await controller.handle_from_radio("node-1", acknowledgment)

    assert first.acknowledged
    assert not second.acknowledged
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
    await controller.stop()

    result = controller.result()
    assert result is not None
    assert result.delivered == 1
    assert result.metrics.unique_application_messages_delivered == 1
    assert result.metrics.delivery_ratio == 1
    assert result.metrics.receiver_deliveries == 2
    assert result.metrics.receivers_per_broadcast == {"1": 2}


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
    legacy.pop("collisionPatchSha256")
    legacy.pop("firmwareBinarySha256")
    legacy.pop("buildArchitecture")
    legacy.pop("upstreamBaseImageDigest")
    legacy["firmwareImageDigest"] = "sha256:legacy-upstream"

    migrated = TrafficRunResult.model_validate(legacy)

    assert migrated.collision_patch_sha256 == "unavailable"
    assert migrated.firmware_binary_sha256 == "unavailable"
    assert migrated.build_architecture == "unavailable"
    assert migrated.upstream_base_image_digest == "sha256:legacy-upstream"


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

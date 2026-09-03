from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from meshtastic.protobuf import mesh_pb2

import backend.app.simulator.service as service_module
from backend.app.gateway import GatewayEvent
from backend.app.metrics import EventBroker, EventType, PacketEvent
from backend.app.models import (
    DirectedLink,
    Scenario,
    TopologyPreset,
    apply_topology_preset,
    default_scenario,
)
from backend.app.provenance import BuildMetadata
from backend.app.runtime import NativeProcessSupervisor, NodeVerification, ProcessRecord
from backend.app.simulator import LifecycleState, SimulationConflict, SimulatorService
from backend.app.traffic import TrafficController, TrafficRunRequest


class FakeGateway:
    def __init__(self) -> None:
        self.sent: list[mesh_pb2.ToRadio] = []

    async def send_to_radio(self, message: mesh_pb2.ToRadio, *, source: str) -> None:
        del source
        copied = mesh_pb2.ToRadio()
        copied.CopyFrom(message)
        self.sent.append(copied)


class FakeMedium:
    def __init__(self) -> None:
        self.links: list[DirectedLink] = []

    async def update_link(self, link: DirectedLink) -> PacketEvent:
        self.links.append(link)
        return PacketEvent(
            sequence=len(self.links),
            monotonicSeconds=float(len(self.links)),
            eventType=EventType.LINK_UPDATED,
            transmitter=link.from_node,
            receiver=link.to_node,
            result="enabled" if link.enabled else "disabled",
        )


class BlockingMedium(FakeMedium):
    def __init__(self) -> None:
        super().__init__()
        self.update_started = asyncio.Event()
        self.release_update = asyncio.Event()

    async def update_link(self, link: DirectedLink) -> PacketEvent:
        self.update_started.set()
        await self.release_update.wait()
        return await super().update_link(link)


class WarmupGateway:
    control_host = "127.0.0.1"
    control_port = 1

    def __init__(self) -> None:
        self.client_disconnected = asyncio.Event()
        self.client_disconnected.set()


def verification(node_id: str) -> NodeVerification:
    return NodeVerification(
        node_id=node_id,
        node_number=1,
        firmware_version="test",
        owner_long_name=node_id,
        owner_short_name="N1",
        role="CLIENT",
        region="US",
        modem_preset="LONG_FAST",
        frequency_slot=20,
        hop_limit=3,
        channel_name="lab",
    )


@pytest.mark.asyncio
async def test_runtime_link_snapshot_is_atomic_with_traffic_start(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = default_scenario(2)
    service.state = LifecycleState.RUNNING
    medium = FakeMedium()
    service.medium = medium  # type: ignore[assignment]
    gateways = {node.id: FakeGateway() for node in service.scenario.nodes}
    service.traffic = TrafficController(
        scenario=service.scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path / "runs",
        settle_seconds=0,
    )

    async def sample_local_stats() -> dict[str, int]:
        return {"node-1": 0, "node-2": 0}

    service._sample_local_stats = sample_local_stats  # type: ignore[method-assign]

    changed = DirectedLink.model_validate(
        {"from": "node-1", "to": "node-2", "enabled": False}
    )
    await service.update_link(changed)
    await service.start_traffic(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=0.1,
            durationSeconds=1,
            payloadBytes=64,
        )
    )

    assert service.traffic.current is not None
    links = service.traffic.current.scenario_snapshot["links"]
    assert isinstance(links, list)
    changed_snapshot = next(
        link for link in links if link["from"] == "node-1" and link["to"] == "node-2"
    )
    assert changed_snapshot["enabled"] is False
    restored = DirectedLink.model_validate(
        {"from": "node-1", "to": "node-2", "enabled": True}
    )
    await service.update_link(restored)
    assert service.traffic.current is not None
    assert [change.link for change in service.traffic.current.topology_changes] == [restored]
    assert service.traffic.current.topology_changes[0].event_sequence == 2
    await service.update_link(restored)
    assert [change.link for change in service.traffic.current.topology_changes] == [restored]
    assert medium.links == [changed, restored]
    await service.traffic.stop()


@pytest.mark.asyncio
async def test_traffic_start_rechecks_lifecycle_after_baseline_sampling(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = default_scenario(2)
    service.state = LifecycleState.RUNNING
    traffic = TrafficController(
        scenario=service.scenario,
        gateways={node.id: FakeGateway() for node in service.scenario.nodes},  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path / "runs",
        settle_seconds=0,
    )
    service.traffic = traffic

    async def sample_after_stop_begins() -> dict[str, int]:
        service.state = LifecycleState.STOPPING
        return {"node-1": 0, "node-2": 0}

    service._sample_local_stats = sample_after_stop_begins  # type: ignore[method-assign]

    with pytest.raises(SimulationConflict) as raised:
        await service.start_traffic(
            TrafficRunRequest(sourceNodes=["node-1"], durationSeconds=1, payloadBytes=64)
        )

    assert raised.value.code == "INVALID_LIFECYCLE_STATE"
    assert traffic.current is None


@pytest.mark.asyncio
async def test_configuration_failure_settles_reconnect_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = default_scenario(2)
    sibling_started = threading.Event()
    sibling_settled = asyncio.Event()
    sibling_cancelled = asyncio.Event()

    class ConfigurationGateway:
        control_host = "127.0.0.1"
        control_port = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self.client_disconnected = asyncio.Event()
            self.client_disconnected.set()
            self.failed = asyncio.Event()
            self.failed.set()

        async def stop(self) -> None:
            if self.node_id != "node-2":
                return
            sibling_started.set()
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                sibling_cancelled.set()
                raise
            sibling_settled.set()
            raise RuntimeError("sibling finished")

        async def start(self) -> None:
            return

    service.gateways = {
        node.id: ConfigurationGateway(node.id) for node in service.scenario.nodes
    }  # type: ignore[assignment]

    def configure_node(**kwargs: object) -> NodeVerification:
        node = kwargs["node"]
        assert hasattr(node, "id")
        return verification(str(node.id))

    def verify_configured_node(**kwargs: object) -> NodeVerification:
        node = kwargs["node"]
        assert hasattr(node, "id")
        if node.id == "node-1":
            assert sibling_started.wait(timeout=1)
            raise RuntimeError("verification failed")
        return verification(str(node.id))

    monkeypatch.setattr(service_module, "configure_and_verify_node", configure_node)
    monkeypatch.setattr(service_module, "verify_node", verify_configured_node)

    with pytest.raises(RuntimeError):
        await service._configure_nodes()

    assert sibling_settled.is_set()
    assert not sibling_cancelled.is_set()


@pytest.mark.asyncio
async def test_node_state_feed_includes_disconnect_and_stop(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)

    await service._on_gateway_event(
        GatewayEvent(node_id="node-1", kind="gateway.client_disconnected", detail="peer")
    )
    await service._on_gateway_event(
        GatewayEvent(node_id="node-1", kind="gateway.stopped", detail="gateway stopped")
    )

    node_events = [
        event for event in service.event_broker.recent() if event.event_type == EventType.NODE_STATE
    ]
    assert [event.result for event in node_events] == [
        "gateway.client_disconnected",
        "gateway.stopped",
    ]


@pytest.mark.asyncio
async def test_unknown_runtime_link_returns_structured_conflict(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = default_scenario(2)
    service.state = LifecycleState.RUNNING
    service.medium = FakeMedium()  # type: ignore[assignment]

    unknown = DirectedLink.model_validate(
        {"from": "node-1", "to": "deleted-node", "enabled": False}
    )
    with pytest.raises(SimulationConflict) as raised:
        await service.update_link(unknown)

    assert raised.value.code == "UNKNOWN_LINK"


@pytest.mark.asyncio
async def test_traffic_stop_waits_for_started_topology_update(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = default_scenario(2)
    service.state = LifecycleState.RUNNING
    medium = BlockingMedium()
    service.medium = medium  # type: ignore[assignment]
    service.traffic = TrafficController(
        scenario=service.scenario,
        gateways={node.id: FakeGateway() for node in service.scenario.nodes},  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path / "runs",
        settle_seconds=10,
    )
    service.traffic.start(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=0.1,
            durationSeconds=1,
            payloadBytes=64,
        )
    )
    changed = DirectedLink.model_validate(
        {"from": "node-1", "to": "node-2", "enabled": False}
    )

    update_task = asyncio.create_task(service.update_link(changed))
    await medium.update_started.wait()
    stop_task = asyncio.create_task(service.stop_traffic())
    await asyncio.sleep(0)
    assert not stop_task.done()

    medium.release_update.set()
    await update_task
    await stop_task

    result = service.traffic.result()
    assert result is not None
    assert result.state == service_module.TrafficRunState.CANCELLED
    assert [change.link for change in result.topology_changes] == [changed]


@pytest.mark.asyncio
async def test_ten_node_line_warmup_is_best_effort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    line = apply_topology_preset(default_scenario(10), TopologyPreset.LINE)
    service.scenario = line.model_copy(
        deep=True, update={"rf": line.rf.model_copy(update={"hop_limit": 7})}
    )
    service.gateways = {
        node.id: WarmupGateway() for node in service.scenario.nodes
    }  # type: ignore[assignment]

    def unavailable_nodeinfo(**_kwargs: object) -> int:
        raise RuntimeError("no route observation")

    monkeypatch.setattr(service_module, "request_node_info", unavailable_nodeinfo)

    missing, expected = await service._warm_up_nodes()

    assert expected == len(
        service.scenario.reachable_pairs(max_hops=service.scenario.rf.hop_limit)
    )
    assert ("node-1", "node-8") in missing
    assert ("node-1", "node-10") not in missing


@pytest.mark.asyncio
async def test_rf_queue_overflow_fails_with_explicit_category(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.state = LifecycleState.RUNNING

    await service._on_gateway_event(
        GatewayEvent(
            node_id="node-1",
            kind="gateway.rf_queue_full",
            detail="RF frame dropped from controller queue",
        )
    )
    assert service._failure_cleanup_task is not None
    await service._failure_cleanup_task

    assert service.state == LifecycleState.FAILED
    assert service.message.startswith("SIMULATOR_OVERLOAD:")


@pytest.mark.asyncio
async def test_process_failure_during_gateway_start_cleans_up_after_start_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "collision-marker"
    marker.touch()
    service = SimulatorService(
        binary_path=tmp_path / "meshtasticd",
        data_root=tmp_path,
        collision_marker=marker,
        warmup_seconds=0,
    )
    service.build_metadata = BuildMetadata(
        firmwareCommit="firmware",
        collisionPatchSha256="patch",
        firmwareBinarySha256="binary",
        buildArchitecture="aarch64",
        clientLibraryVersion="2.7.11",
        upstreamBaseImageDigest="sha256:base",
    )
    gateway_started = asyncio.Event()
    release_gateways = asyncio.Event()
    gateways: list[StartupGateway] = []

    class StartupGateway:
        def __init__(self, **_kwargs: object) -> None:
            self.listening = False
            gateways.append(self)

        def reserve_public_listener(self) -> None:
            return

        async def start(self) -> None:
            gateway_started.set()
            await release_gateways.wait()
            self.listening = True

        async def stop(self) -> None:
            self.listening = False

    async def start_processes(_scenario: Scenario) -> dict[str, SimpleNamespace]:
        return {
            node.id: SimpleNamespace(internal_port=46000 + index)
            for index, node in enumerate(service.scenario.nodes, start=1)
        }

    async def stop_processes() -> None:
        return

    monkeypatch.setattr(service_module, "NodeGateway", StartupGateway)
    monkeypatch.setattr(service.supervisor, "start", start_processes)
    monkeypatch.setattr(service.supervisor, "stop", stop_processes)

    start_task = asyncio.create_task(service.start())
    await gateway_started.wait()
    await service._on_process_failure("node-1", 1)

    assert service.state == LifecycleState.FAILED
    assert service._failure_cleanup_task is None
    release_gateways.set()
    with pytest.raises(RuntimeError, match="Firmware node node-1 exited"):
        await start_task

    assert gateways
    assert not any(gateway.listening for gateway in gateways)
    assert service.gateways == {}


@pytest.mark.asyncio
async def test_runtime_failure_cleanup_waits_for_lifecycle_command(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    stopped = asyncio.Event()

    class StoppableGateway:
        async def stop(self) -> None:
            stopped.set()

    service.state = LifecycleState.RUNNING
    service.gateways = {"node-1": StoppableGateway()}  # type: ignore[dict-item]

    async with service._lifecycle_lock:
        service._schedule_runtime_failure("GATEWAY_FAILED", "boom")
        cleanup_task = service._failure_cleanup_task
        assert cleanup_task is not None
        await asyncio.sleep(0)
        assert not stopped.is_set()

    await cleanup_task
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_retained_rx_bad_counter_is_baselined_before_collision_event(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = service.scenario.model_copy(update={"fresh_state": False})
    service.node_metrics = {
        "node-1": service_module._NodeMetrics(
            rx_bad_initialized=service.scenario.fresh_state
        )
    }

    async def report(total: int) -> None:
        response = mesh_pb2.FromRadio()
        response.packet.decoded.portnum = service_module.portnums_pb2.TELEMETRY_APP
        telemetry = service_module.telemetry_pb2.Telemetry()
        telemetry.local_stats.num_packets_rx_bad = total
        response.packet.decoded.payload = telemetry.SerializeToString()
        await service._on_from_radio("node-1", response)

    await report(7)
    assert not [
        event
        for event in service.event_broker.recent()
        if event.event_type == EventType.COLLISION
    ]

    await report(8)
    collisions = [
        event
        for event in service.event_broker.recent()
        if event.event_type == EventType.COLLISION
    ]
    assert len(collisions) == 1
    assert collisions[0].receiver == "node-1"


@pytest.mark.asyncio
async def test_local_stats_sampler_waits_for_fresh_per_node_responses(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.node_metrics = {
        "node-1": service_module._NodeMetrics(),
        "node-2": service_module._NodeMetrics(),
    }
    service.verifications = {
        "node-1": SimpleNamespace(node_number=1),
        "node-2": SimpleNamespace(node_number=2),
    }  # type: ignore[assignment]

    class StatsGateway:
        def __init__(self, node_id: str, rx_bad: int, rx_duplicate: int) -> None:
            self.node_id = node_id
            self.rx_bad = rx_bad
            self.rx_duplicate = rx_duplicate
            self.requests: list[mesh_pb2.ToRadio] = []
            self.reply_task: asyncio.Task[None] | None = None

        async def send_to_radio(self, request: mesh_pb2.ToRadio, *, source: str) -> None:
            assert source == "controller.local-stats"
            copied = mesh_pb2.ToRadio()
            copied.CopyFrom(request)
            self.requests.append(copied)
            unsolicited = mesh_pb2.FromRadio()
            unsolicited.packet.decoded.portnum = service_module.portnums_pb2.TELEMETRY_APP
            unsolicited_telemetry = service_module.telemetry_pb2.Telemetry()
            unsolicited_telemetry.local_stats.num_packets_rx_bad = 999
            unsolicited.packet.decoded.payload = unsolicited_telemetry.SerializeToString()
            await service._on_from_radio(self.node_id, unsolicited)

            async def reply() -> None:
                await asyncio.sleep(0)
                response = mesh_pb2.FromRadio()
                response.packet.decoded.portnum = service_module.portnums_pb2.TELEMETRY_APP
                response.packet.decoded.request_id = request.packet.id
                telemetry = service_module.telemetry_pb2.Telemetry()
                telemetry.local_stats.num_packets_rx_bad = self.rx_bad
                telemetry.local_stats.num_rx_dupe = self.rx_duplicate
                response.packet.decoded.payload = telemetry.SerializeToString()
                await service._on_from_radio(self.node_id, response)

            self.reply_task = asyncio.create_task(reply())

    gateways = {
        "node-1": StatsGateway("node-1", 3, 2),
        "node-2": StatsGateway("node-2", 5, 4),
    }
    service.gateways = gateways  # type: ignore[assignment]

    result = await service._sample_local_stats()

    assert result.totals == {"node-1": 3, "node-2": 5}
    assert result.duplicate_totals == {"node-1": 2, "node-2": 4}
    assert result.missing_nodes == ()
    for gateway in gateways.values():
        packet = gateway.requests[0].packet
        assert packet.to in {1, 2}
        assert packet.decoded.portnum == service_module.portnums_pb2.TELEMETRY_APP
        assert packet.decoded.want_response
        assert service._local_stats_response_ids[gateway.node_id] == packet.id
        assert gateway.reply_task is not None and gateway.reply_task.done()


@pytest.mark.asyncio
async def test_local_stats_sampler_retries_and_preserves_partial_results(tmp_path: Path) -> None:
    service = SimulatorService(
        data_root=tmp_path,
        warmup_seconds=0,
        local_stats_deadline_seconds=0.08,
        local_stats_retry_seconds=0.01,
    )
    service.node_metrics = {
        "node-1": service_module._NodeMetrics(),
        "node-2": service_module._NodeMetrics(),
    }
    service.verifications = {
        "node-1": SimpleNamespace(node_number=1),
        "node-2": SimpleNamespace(node_number=2),
    }  # type: ignore[assignment]

    class RetryingStatsGateway:
        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self.request_count = 0

        async def send_to_radio(self, request: mesh_pb2.ToRadio, *, source: str) -> None:
            assert source == "controller.local-stats"
            self.request_count += 1
            if self.node_id != "node-1" or self.request_count != 2:
                return
            response = mesh_pb2.FromRadio()
            response.packet.decoded.portnum = service_module.portnums_pb2.TELEMETRY_APP
            response.packet.decoded.request_id = request.packet.id
            telemetry = service_module.telemetry_pb2.Telemetry()
            telemetry.local_stats.num_packets_rx_bad = 7
            response.packet.decoded.payload = telemetry.SerializeToString()
            await service._on_from_radio(self.node_id, response)

    gateways = {
        "node-1": RetryingStatsGateway("node-1"),
        "node-2": RetryingStatsGateway("node-2"),
    }
    service.gateways = gateways  # type: ignore[assignment]

    result = await service._sample_local_stats()

    assert result.totals == {"node-1": 7}
    assert result.missing_nodes == ("node-2",)
    assert gateways["node-1"].request_count == 2
    assert gateways["node-2"].request_count > 1


@pytest.mark.asyncio
async def test_stop_waits_for_in_flight_failure_cleanup(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.state = LifecycleState.FAILED
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def cleanup() -> None:
        cleanup_started.set()
        await release_cleanup.wait()

    service._failure_cleanup_task = asyncio.create_task(cleanup())
    stop_task = asyncio.create_task(service.stop())
    await cleanup_started.wait()
    await asyncio.sleep(0)
    assert not stop_task.done()

    release_cleanup.set()
    result = await stop_task

    assert result.state == LifecycleState.STOPPED
    assert service._failure_cleanup_task is None


@pytest.mark.asyncio
async def test_stop_cleans_resources_created_by_concurrent_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "collision-marker"
    marker.touch()
    service = SimulatorService(
        binary_path=tmp_path / "meshtasticd",
        data_root=tmp_path,
        collision_marker=marker,
        warmup_seconds=0,
    )
    service.build_metadata = BuildMetadata(
        firmwareCommit="firmware",
        collisionPatchSha256="patch",
        firmwareBinarySha256="binary",
        buildArchitecture="aarch64",
        clientLibraryVersion="2.7.11",
        upstreamBaseImageDigest="sha256:base",
    )
    service.state = LifecycleState.FAILED
    cleanup_release = asyncio.Event()

    class RestartGateway:
        def __init__(self, **_kwargs: object) -> None:
            self.listening = False
            started_gateways.append(self)

        def reserve_public_listener(self) -> None:
            return

        async def start(self) -> None:
            self.listening = True

        async def stop(self) -> None:
            self.listening = False

        async def enable_public_clients(self) -> None:
            return

    started_gateways: list[RestartGateway] = []

    class RestartMedium:
        def __init__(self, **_kwargs: object) -> None:
            return

        async def start(self) -> None:
            return

        async def stop(self) -> None:
            return

    async def prior_cleanup() -> None:
        await cleanup_release.wait()

    async def start_processes(_scenario: Scenario) -> dict[str, SimpleNamespace]:
        return {
            node.id: SimpleNamespace(internal_port=46000 + index)
            for index, node in enumerate(service.scenario.nodes, start=1)
        }

    async def configure_nodes() -> dict[str, NodeVerification]:
        return {node.id: verification(node.id) for node in service.scenario.nodes}

    async def warm_up_nodes() -> tuple[set[tuple[str, str]], int]:
        return set(), 0

    monkeypatch.setattr(service_module, "NodeGateway", RestartGateway)
    monkeypatch.setattr(service_module, "DirectedMedium", RestartMedium)
    monkeypatch.setattr(service.supervisor, "start", start_processes)
    monkeypatch.setattr(service, "_configure_nodes", configure_nodes)
    monkeypatch.setattr(service, "_warm_up_nodes", warm_up_nodes)
    service._failure_cleanup_task = asyncio.create_task(prior_cleanup())

    await service._lifecycle_lock.acquire()
    start_task = asyncio.create_task(service.start())
    await asyncio.sleep(0)
    stop_task = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    cleanup_release.set()
    await asyncio.sleep(0)
    service._lifecycle_lock.release()

    await start_task
    result = await stop_task

    assert result.state == LifecycleState.STOPPED
    assert started_gateways
    assert not any(gateway.listening for gateway in started_gateways)
    assert service.gateways == {}


@pytest.mark.asyncio
async def test_stop_retries_cleanup_after_failure_task_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.state = LifecycleState.FAILED
    cleanup_called = asyncio.Event()

    async def failed_cleanup() -> None:
        raise RuntimeError("cleanup failed")

    async def retry_cleanup() -> None:
        cleanup_called.set()

    service._failure_cleanup_task = asyncio.create_task(failed_cleanup())
    monkeypatch.setattr(service, "_cleanup_resources", retry_cleanup)

    result = await service.stop()

    assert result.state == LifecycleState.STOPPED
    assert cleanup_called.is_set()
    assert service._failure_cleanup_task is None


@pytest.mark.asyncio
async def test_supervisor_stop_waits_for_process_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = tmp_path / "meshtasticd"
    binary.write_text("test binary", encoding="utf-8")
    supervisor = NativeProcessSupervisor(binary_path=binary, data_root=tmp_path / "nodes")
    allocation_started = asyncio.Event()
    release_allocation = asyncio.Event()

    async def start_one(_record: ProcessRecord, *, erase: bool) -> None:
        del erase
        allocation_started.set()
        await release_allocation.wait()

    monkeypatch.setattr(supervisor, "_start_one", start_one)
    start_task = asyncio.create_task(supervisor.start(default_scenario(2)))
    await allocation_started.wait()
    stop_task = asyncio.create_task(supervisor.stop())
    await asyncio.sleep(0)

    assert not stop_task.done()
    release_allocation.set()
    await start_task
    await stop_task

    assert supervisor.records == {}


@pytest.mark.asyncio
async def test_archived_process_logs_survive_cleanup_until_reset(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    record = ProcessRecord(
        node_id="node-1",
        hardware_id=1,
        internal_port=46001,
        data_directory=tmp_path / "node-1" / "state",
        stdout_path=tmp_path / "node-1" / "stdout.log",
        stderr_path=tmp_path / "node-1" / "stderr.log",
    )
    record.stdout_lines.append("started")
    record.stderr_lines.append("fatal detail")
    service.supervisor.records[record.node_id] = record

    async def drain_final_line() -> None:
        await asyncio.sleep(0)
        record.stderr_lines.append("final buffered detail")

    drain_task = asyncio.create_task(drain_final_line())
    record.tasks.add(drain_task)
    drain_task.add_done_callback(record.tasks.discard)

    await service.supervisor.stop()

    assert service.supervisor.has_logs("node-1")
    assert service.supervisor.recent_logs("node-1", stream="stdout") == ["started"]
    assert service.supervisor.recent_logs("node-1", stream="stderr") == [
        "fatal detail",
        "final buffered detail",
    ]

    await service.reset()
    assert not service.supervisor.has_logs("node-1")

"""Simulation lifecycle, node state, and subsystem coordination."""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from meshtastic.protobuf import mesh_pb2, portnums_pb2, telemetry_pb2
from pydantic import BaseModel, ConfigDict, Field

from backend.app.gateway import GatewayEvent, GatewayState, NodeGateway
from backend.app.metrics import EventBroker, EventType, PacketEvent
from backend.app.models import (
    DirectedLink,
    Scenario,
    TopologyPreset,
    apply_topology_preset,
    default_scenario,
)
from backend.app.runtime import (
    NativeProcessSupervisor,
    NodeProcessState,
    NodeVerification,
    configure_and_verify_node,
    request_node_info,
    verify_node,
)
from backend.app.traffic import TrafficController, TrafficRunRequest, TrafficRunResult, TrafficRunState

from .medium import DirectedMedium

FIRMWARE_IMAGE_DIGEST = "sha256:23e92b1331a3a471eaef0c63cbca4365ca40b3111a9781cfdbe5a5114e5773d4"
COLLISION_MARKER_DEFAULT = "/usr/share/meshtastic-lab/native-collision-enabled"
LOGGER = logging.getLogger(__name__)


class LifecycleState(StrEnum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    WARMING_UP = "WARMING_UP"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    FAILED = "FAILED"


class SimulationConflict(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class CommandResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_id: str = Field(alias="commandId")
    state: LifecycleState
    detail: str


class CapabilityView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    collision_model: str = Field(alias="collisionModel")
    collision_available: bool = Field(alias="collisionAvailable")
    collision_detail: str = Field(alias="collisionDetail")
    maximum_nodes: int = Field(default=10, alias="maximumNodes")
    supported_container_architectures: list[str] = Field(
        default_factory=lambda: ["linux/amd64", "linux/arm64"],
        alias="supportedContainerArchitectures",
    )
    firmware_image_digest: str = Field(default=FIRMWARE_IMAGE_DIGEST, alias="firmwareImageDigest")


class NodeView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    role: str
    process_state: NodeProcessState = Field(alias="processState")
    process_id: int | None = Field(alias="processId")
    gateway_state: GatewayState = Field(alias="gatewayState")
    external_client_connected: bool = Field(alias="externalClientConnected")
    public_endpoint: str = Field(alias="publicEndpoint")
    firmware_version: str | None = Field(alias="firmwareVersion")
    node_number: int | None = Field(alias="nodeNumber")
    transmit_count: int = Field(alias="transmitCount")
    receive_count: int = Field(alias="receiveCount")
    failed_receive_count: int = Field(alias="failedReceiveCount")
    duplicate_receive_count: int = Field(alias="duplicateReceiveCount")
    channel_utilization: float | None = Field(alias="channelUtilization")


class LifecycleView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state: LifecycleState
    simulation_id: str | None = Field(alias="simulationId")
    message: str
    warming_up_until: datetime | None = Field(alias="warmingUpUntil")
    active_traffic_run_id: str | None = Field(alias="activeTrafficRunId")


class _NodeMetrics:
    def __init__(self) -> None:
        self.tx = 0
        self.rx = 0
        self.rx_bad = 0
        self.rx_duplicate = 0
        self.channel_utilization: float | None = None


class SimulatorService:
    """Serialize lifecycle commands and keep cleanup bounded."""

    def __init__(
        self,
        *,
        binary_path: Path | None = None,
        data_root: Path | None = None,
        collision_marker: Path | None = None,
        warmup_seconds: float = 5.0,
    ) -> None:
        binary = binary_path or Path(os.environ.get("MESHTASTICD_BIN", "/usr/bin/meshtasticd"))
        root = data_root or Path(os.environ.get("MESHTASTIC_LAB_DATA", "/data"))
        marker = collision_marker or Path(
            os.environ.get("MESHTASTIC_COLLISION_MARKER", COLLISION_MARKER_DEFAULT)
        )
        self.data_root = root
        self.results_root = root / "runs"
        self.collision_marker = marker
        self.warmup_seconds = warmup_seconds
        self.scenario = default_scenario()
        self.state = LifecycleState.STOPPED
        self.simulation_id: str | None = None
        self.message = "Ready to start"
        self.warming_up_until: datetime | None = None
        self.event_broker = EventBroker()
        self.supervisor = NativeProcessSupervisor(
            binary_path=binary,
            data_root=root / "nodes",
            failure_handler=self._on_process_failure,
        )
        self.gateways: dict[str, NodeGateway] = {}
        self.verifications: dict[str, NodeVerification] = {}
        self.node_metrics: dict[str, _NodeMetrics] = {}
        self.nodeinfo_observations: set[tuple[str, str]] = set()
        self._nodeinfo_changed = asyncio.Event()
        self.medium: DirectedMedium | None = None
        self.traffic: TrafficController | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._failure_cleanup_task: asyncio.Task[None] | None = None
        self._event_loop_lag_task: asyncio.Task[None] | None = None

    def capabilities(self) -> CapabilityView:
        available = self.collision_marker.is_file()
        return CapabilityView(
            collisionModel="native",
            collisionAvailable=available,
            collisionDetail=(
                "Native SimRadio overlap handling was compiled and probed."
                if available
                else "Native collision marker is absent. Simulation start is disabled."
            ),
        )

    def lifecycle(self) -> LifecycleView:
        active_run = None
        if self.traffic is not None and self.traffic.current is not None:
            if self.traffic.state in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}:
                active_run = self.traffic.current.run_id
        return LifecycleView(
            state=self.state,
            simulationId=self.simulation_id,
            message=self.message,
            warmingUpUntil=self.warming_up_until,
            activeTrafficRunId=active_run,
        )

    async def start(self) -> CommandResult:
        command_id = str(uuid.uuid4())
        async with self._lifecycle_lock:
            if self.state in {LifecycleState.RUNNING, LifecycleState.WARMING_UP}:
                return CommandResult(commandId=command_id, state=self.state, detail="already running")
            if self.state not in {LifecycleState.STOPPED, LifecycleState.FAILED}:
                raise SimulationConflict("INVALID_LIFECYCLE_STATE", f"cannot start from {self.state}")
            if not self.capabilities().collision_available:
                raise SimulationConflict(
                    "NATIVE_COLLISION_UNAVAILABLE",
                    "the runtime does not contain a verified collision-enabled native firmware build",
                )

            self.simulation_id = str(uuid.uuid4())
            self.state = LifecycleState.STARTING
            self.message = "Starting native firmware processes"
            self._publish_lifecycle()
            try:
                records = await self.supervisor.start(self.scenario)
                self.node_metrics = {node.id: _NodeMetrics() for node in self.scenario.nodes}
                self.gateways = {
                    node.id: NodeGateway(
                        node_id=node.id,
                        downstream_host="127.0.0.1",
                        downstream_port=records[node.id].internal_port,
                        public_host="0.0.0.0",
                        public_port=node.api_port,
                        event_handler=self._on_gateway_event,
                        from_radio_handler=self._on_from_radio,
                    )
                    for node in self.scenario.nodes
                }
                await asyncio.gather(*(gateway.start() for gateway in self.gateways.values()))
                self.message = "Configuring and verifying native nodes"
                self.verifications = await self._configure_nodes()
                hardware_ids = {
                    node_id: verification.node_number
                    for node_id, verification in self.verifications.items()
                }
                self.traffic = TrafficController(
                    scenario=self.scenario,
                    gateways=self.gateways,
                    hardware_ids=hardware_ids,
                    event_broker=self.event_broker,
                    results_root=self.results_root,
                )
                self.medium = DirectedMedium(
                    scenario=self.scenario,
                    gateways=self.gateways,
                    event_broker=self.event_broker,
                    hardware_ids=hardware_ids,
                    transmission_handler=self.traffic.record_rf_transmission,
                    drop_handler=self.traffic.record_drop,
                )
                await self.medium.start()
                self.state = LifecycleState.WARMING_UP
                self.message = "Firmware is exchanging NodeInfo and establishing routes"
                warmup_deadline = self._warmup_deadline_seconds()
                self.warming_up_until = datetime.fromtimestamp(
                    datetime.now(UTC).timestamp() + warmup_deadline + self.warmup_seconds, UTC
                )
                self._publish_lifecycle()
                await self._warm_up_nodes()
                self.state = LifecycleState.RUNNING
                self.message = f"{len(self.gateways)} native nodes are running"
                self.warming_up_until = None
                self._event_loop_lag_task = asyncio.create_task(
                    self._measure_event_loop_lag(), name="event-loop-lag"
                )
                self._publish_lifecycle()
                return CommandResult(commandId=command_id, state=self.state, detail=self.message)
            except Exception as exc:
                self.state = LifecycleState.FAILED
                self.message = f"Startup failed: {exc}"
                self._publish_lifecycle()
                await self._cleanup_resources()
                raise

    async def stop(self) -> CommandResult:
        command_id = str(uuid.uuid4())
        async with self._lifecycle_lock:
            if self.state == LifecycleState.STOPPED:
                return CommandResult(commandId=command_id, state=self.state, detail="already stopped")
            self.state = LifecycleState.STOPPING
            self.message = "Stopping traffic, gateways, and native processes"
            self._publish_lifecycle()
            await self._cleanup_resources()
            self.state = LifecycleState.STOPPED
            self.message = "Stopped"
            self.simulation_id = None
            self.warming_up_until = None
            self._publish_lifecycle()
            return CommandResult(commandId=command_id, state=self.state, detail=self.message)

    async def reset(self) -> CommandResult:
        if self.state != LifecycleState.STOPPED:
            raise SimulationConflict("INVALID_LIFECYCLE_STATE", "reset is allowed only while stopped")
        self.scenario = default_scenario()
        self.message = "Scenario reset to the five-node full mesh"
        return CommandResult(commandId=str(uuid.uuid4()), state=self.state, detail=self.message)

    def replace_scenario(self, scenario: Scenario) -> Scenario:
        if self.state != LifecycleState.STOPPED:
            raise SimulationConflict(
                "SCENARIO_LOCKED",
                "node, identity, channel, and RF settings are editable only while stopped",
            )
        self.scenario = scenario
        return self.scenario

    async def update_link(self, link: DirectedLink) -> DirectedLink:
        if self.state != LifecycleState.RUNNING or self.medium is None:
            raise SimulationConflict("INVALID_LIFECYCLE_STATE", "runtime links require RUNNING state")
        await self.medium.update_link(link)
        links = [
            link if (current.from_node, current.to_node) == (link.from_node, link.to_node) else current
            for current in self.scenario.links
        ]
        self.scenario = self.scenario.model_copy(update={"links": links})
        return link

    async def apply_topology(self, preset: TopologyPreset) -> Scenario:
        updated = apply_topology_preset(self.scenario, preset)
        if self.state == LifecycleState.STOPPED:
            self.scenario = updated
            return updated
        if self.state != LifecycleState.RUNNING or self.medium is None:
            raise SimulationConflict("INVALID_LIFECYCLE_STATE", f"cannot apply topology from {self.state}")
        await self.medium.apply_links(updated.links)
        self.scenario = updated
        return updated

    def start_traffic(self, request: TrafficRunRequest) -> str:
        if self.state != LifecycleState.RUNNING or self.traffic is None:
            raise SimulationConflict("INVALID_LIFECYCLE_STATE", "traffic requires RUNNING state")
        try:
            return self.traffic.start(request)
        except RuntimeError as exc:
            raise SimulationConflict("TRAFFIC_RUN_ACTIVE", str(exc)) from exc
        except ValueError as exc:
            raise SimulationConflict("INVALID_TRAFFIC_REQUEST", str(exc)) from exc

    async def stop_traffic(self) -> None:
        if self.traffic is not None:
            await self.traffic.stop()

    def traffic_result(self, run_id: str) -> TrafficRunResult:
        if self.traffic is not None and self.traffic.current is not None:
            if self.traffic.current.run_id == run_id:
                return self.traffic.current
        path = self.results_root / f"{run_id}.json"
        if not path.is_file():
            raise FileNotFoundError(run_id)
        return TrafficRunResult.model_validate_json(path.read_text(encoding="utf-8"))

    def completed_runs(self) -> list[str]:
        if not self.results_root.is_dir():
            return []
        return sorted(path.stem for path in self.results_root.glob("*.json"))

    def nodes(self) -> list[NodeView]:
        views: list[NodeView] = []
        for node in self.scenario.nodes:
            record = self.supervisor.records.get(node.id)
            gateway = self.gateways.get(node.id)
            verification = self.verifications.get(node.id)
            metrics = self.node_metrics.get(node.id, _NodeMetrics())
            views.append(
                NodeView(
                    id=node.id,
                    name=node.display_name,
                    role=node.role.value,
                    processState=(record.state if record is not None else NodeProcessState.STOPPED),
                    processId=record.pid if record is not None else None,
                    gatewayState=(gateway.state if gateway is not None else GatewayState.STOPPED),
                    externalClientConnected=(gateway.external_connected if gateway is not None else False),
                    publicEndpoint=f"127.0.0.1:{node.api_port}",
                    firmwareVersion=(verification.firmware_version if verification is not None else None),
                    nodeNumber=(verification.node_number if verification is not None else None),
                    transmitCount=metrics.tx,
                    receiveCount=metrics.rx,
                    failedReceiveCount=metrics.rx_bad,
                    duplicateReceiveCount=metrics.rx_duplicate,
                    channelUtilization=metrics.channel_utilization,
                )
            )
        return views

    async def _configure_nodes(self) -> dict[str, NodeVerification]:
        async def configure(node_id: str) -> NodeVerification:
            node = next(candidate for candidate in self.scenario.nodes if candidate.id == node_id)
            verification = await asyncio.to_thread(
                configure_and_verify_node,
                hostname="127.0.0.1",
                port=node.api_port,
                node=node,
                rf=self.scenario.rf,
                channel=self.scenario.channel,
                reboot_after_apply=True,
            )
            await asyncio.wait_for(self.gateways[node_id].client_disconnected.wait(), timeout=5)
            return verification

        await asyncio.gather(*(configure(node.id) for node in self.scenario.nodes))

        async def reconnect_and_verify(node_id: str) -> NodeVerification:
            gateway = self.gateways[node_id]
            await asyncio.wait_for(gateway.failed.wait(), timeout=12)
            node = next(candidate for candidate in self.scenario.nodes if candidate.id == node_id)
            for _attempt in range(3):
                await gateway.stop()
                await gateway.start()
                verification = await asyncio.to_thread(
                    verify_node,
                    hostname="127.0.0.1",
                    port=node.api_port,
                    node=node,
                    rf=self.scenario.rf,
                    channel=self.scenario.channel,
                )
                await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)
                try:
                    await asyncio.wait_for(gateway.failed.wait(), timeout=11)
                except TimeoutError:
                    return verification
            raise RuntimeError(f"{node_id} did not stabilize after configuration restarts")

        values = await asyncio.gather(
            *(reconnect_and_verify(node.id) for node in self.scenario.nodes)
        )
        return {verification.node_id: verification for verification in values}

    async def _on_gateway_event(self, event: GatewayEvent) -> None:
        if event.kind in {"gateway.started", "gateway.failed", "gateway.client_connected"}:
            self.event_broker.publish(
                PacketEvent(
                    monotonicSeconds=time.monotonic(),
                    eventType=EventType.NODE_STATE,
                    transmitter=event.node_id,
                    result=event.kind,
                    detail=event.detail,
                )
            )
        if event.kind == "gateway.failed" and self.state in {
            LifecycleState.WARMING_UP,
            LifecycleState.RUNNING,
        }:
            self.state = LifecycleState.FAILED
            self.message = f"Gateway for {event.node_id} failed: {event.detail}"
            self._publish_lifecycle()
            if self._failure_cleanup_task is None or self._failure_cleanup_task.done():
                self._failure_cleanup_task = asyncio.create_task(
                    self._cleanup_resources(), name="failed-gateway-cleanup"
                )

    async def _on_from_radio(self, node_id: str, message: mesh_pb2.FromRadio) -> None:
        if self.traffic is not None:
            await self.traffic.handle_from_radio(node_id, message)
        if message.WhichOneof("payload_variant") != "packet":
            return
        packet = message.packet
        if (
            packet.WhichOneof("payload_variant") == "decoded"
            and packet.decoded.portnum == portnums_pb2.NODEINFO_APP
        ):
            source_number = int(getattr(packet, "from"))
            source = next(
                (
                    candidate
                    for candidate, verification in self.verifications.items()
                    if verification.node_number == source_number
                ),
                None,
            )
            if source is not None and source != node_id:
                self.nodeinfo_observations.add((source, node_id))
                self._nodeinfo_changed.set()
        if (
            packet.WhichOneof("payload_variant") == "decoded"
            and packet.decoded.portnum == portnums_pb2.TELEMETRY_APP
        ):
            telemetry = telemetry_pb2.Telemetry()
            try:
                telemetry.ParseFromString(packet.decoded.payload)
            except Exception:
                return
            metrics = self.node_metrics.get(node_id)
            if metrics is None:
                return
            variant = telemetry.WhichOneof("variant")
            if variant == "local_stats":
                previous_bad = metrics.rx_bad
                metrics.tx = telemetry.local_stats.num_packets_tx
                metrics.rx = telemetry.local_stats.num_packets_rx
                metrics.rx_bad = telemetry.local_stats.num_packets_rx_bad
                metrics.rx_duplicate = telemetry.local_stats.num_rx_dupe
                if self.traffic is not None:
                    self.traffic.record_failed_receptions(
                        node_id, telemetry.local_stats.num_packets_rx_bad
                    )
                if telemetry.local_stats.num_packets_rx_bad > previous_bad:
                    self.event_broker.publish(
                        PacketEvent(
                            monotonicSeconds=time.monotonic(),
                            eventType=EventType.COLLISION,
                            receiver=node_id,
                            result="native-rx-bad",
                            detail="Firmware local statistics incremented num_packets_rx_bad",
                        )
                    )
            elif variant == "device_metrics":
                metrics.channel_utilization = telemetry.device_metrics.channel_utilization

    async def _warm_up_nodes(self) -> None:
        expected = self.scenario.reachable_pairs()
        if not expected:
            return
        self.nodeinfo_observations.clear()
        deadline_seconds = self._warmup_deadline_seconds()
        try:
            async with asyncio.timeout(deadline_seconds):
                for node in self.scenario.nodes:
                    source_expected = {pair for pair in expected if pair[0] == node.id}
                    while source_expected - self.nodeinfo_observations:
                        await asyncio.to_thread(
                            request_node_info,
                            hostname="127.0.0.1",
                            port=node.api_port,
                        )
                        await self.gateways[node.id].client_disconnected.wait()
                        if source_expected.issubset(self.nodeinfo_observations):
                            break
                        self._nodeinfo_changed.clear()
                        if source_expected.issubset(self.nodeinfo_observations):
                            break
                        try:
                            await asyncio.wait_for(self._nodeinfo_changed.wait(), timeout=3)
                        except TimeoutError:
                            continue
        except TimeoutError as exc:
            missing = sorted(expected - self.nodeinfo_observations)
            raise RuntimeError(
                f"firmware warm-up did not observe reachable pairs within "
                f"{deadline_seconds}s: {missing}"
            ) from exc
        await asyncio.sleep(self.warmup_seconds)

    def _warmup_deadline_seconds(self) -> float:
        return min(120, 30 + 10 * len(self.scenario.nodes))

    async def _on_process_failure(self, node_id: str, return_code: int | None) -> None:
        if self.state in {LifecycleState.STOPPING, LifecycleState.STOPPED, LifecycleState.FAILED}:
            return
        self.state = LifecycleState.FAILED
        self.message = f"Firmware node {node_id} exited with code {return_code}"
        self._publish_lifecycle()
        if self._failure_cleanup_task is None or self._failure_cleanup_task.done():
            self._failure_cleanup_task = asyncio.create_task(
                self._cleanup_resources(), name="failed-simulation-cleanup"
            )

    async def _cleanup_resources(self) -> None:
        lag_task, self._event_loop_lag_task = self._event_loop_lag_task, None
        if lag_task is not None:
            lag_task.cancel()
            await asyncio.gather(lag_task, return_exceptions=True)
        if self.traffic is not None:
            await self.traffic.stop()
        if self.medium is not None:
            await self.medium.stop()
        if self.gateways:
            await asyncio.gather(*(gateway.stop() for gateway in self.gateways.values()))
        await self.supervisor.stop()
        self.medium = None
        self.traffic = None
        self.gateways.clear()
        self.verifications.clear()

    async def _measure_event_loop_lag(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            expected = loop.time() + 1
            await asyncio.sleep(1)
            lag_ms = max(0.0, (loop.time() - expected) * 1000)
            if self.traffic is not None:
                self.traffic.set_event_loop_lag(lag_ms)

    def _publish_lifecycle(self) -> None:
        LOGGER.info(
            "simulation lifecycle transition",
            extra={
                "simulation_id": self.simulation_id,
                "lifecycle_transition": self.state.value,
                "error_category": "simulation" if self.state == LifecycleState.FAILED else None,
            },
        )
        self.event_broker.publish(
            PacketEvent(
                monotonicSeconds=time.monotonic(),
                eventType=EventType.LIFECYCLE,
                result=self.state.value,
                detail=self.message,
            )
        )

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
from typing import TypeVar

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
from backend.app.provenance import DEFAULT_METADATA_PATH, BuildMetadata, load_build_metadata
from backend.app.runtime import (
    NativeProcessSupervisor,
    NodeProcessState,
    NodeVerification,
    configure_and_verify_node,
    request_node_info,
    verify_node,
)
from backend.app.traffic import (
    FailedReceptionSample,
    PacketIdQuarantineCapacityError,
    TrafficController,
    TrafficRunRequest,
    TrafficRunResult,
    TrafficRunState,
    TrafficRunSummary,
    summarize_result,
)

from .medium import DirectedMedium

COLLISION_MARKER_DEFAULT = "/usr/share/meshtastic-lab/native-collision-enabled"
MAX_VOLATILE_RESULTS = 8
LOGGER = logging.getLogger(__name__)
_TaskResult = TypeVar("_TaskResult")


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
    provenance_available: bool = Field(alias="provenanceAvailable")
    firmware_commit: str = Field(alias="firmwareCommit")
    collision_patch_sha256: str = Field(alias="collisionPatchSha256")
    firmware_binary_sha256: str = Field(alias="firmwareBinarySha256")
    build_architecture: str = Field(alias="buildArchitecture")
    client_library_version: str = Field(alias="clientLibraryVersion")
    upstream_base_image_digest: str = Field(alias="upstreamBaseImageDigest")
    meshtasticator_commit: str = Field(alias="meshtasticatorCommit")


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
    def __init__(self, *, rx_bad_initialized: bool = False) -> None:
        self.tx = 0
        self.rx = 0
        self.rx_bad = 0
        self.rx_bad_initialized = rx_bad_initialized
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
        build_metadata_path: Path | None = None,
        warmup_seconds: float = 5.0,
        local_stats_deadline_seconds: float = 10.0,
        local_stats_retry_seconds: float = 1.0,
    ) -> None:
        binary = binary_path or Path(os.environ.get("MESHTASTICD_BIN", "/usr/bin/meshtasticd"))
        root = data_root or Path(os.environ.get("MESHTASTIC_LAB_DATA", "/data"))
        marker = collision_marker or Path(
            os.environ.get("MESHTASTIC_COLLISION_MARKER", COLLISION_MARKER_DEFAULT)
        )
        metadata_path = build_metadata_path or Path(
            os.environ.get("MESHTASTIC_BUILD_METADATA", str(DEFAULT_METADATA_PATH))
        )
        if local_stats_deadline_seconds <= 0 or local_stats_retry_seconds <= 0:
            raise ValueError("local-stat sampling intervals must be positive")
        self.data_root = root
        self.results_root = root / "runs"
        self.collision_marker = marker
        self.build_metadata: BuildMetadata = load_build_metadata(metadata_path)
        self.warmup_seconds = warmup_seconds
        self.local_stats_deadline_seconds = local_stats_deadline_seconds
        self.local_stats_retry_seconds = local_stats_retry_seconds
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
        self._local_stats_condition = asyncio.Condition()
        self._local_stats_response_ids: dict[str, int] = {}
        self._control_packet_id = 0x4D4C0000
        self.medium: DirectedMedium | None = None
        self.traffic: TrafficController | None = None
        self._volatile_results: dict[str, TrafficRunResult] = {}
        self._last_traffic_summary: TrafficRunSummary | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._cleanup_lock = asyncio.Lock()
        self._topology_lock = asyncio.Lock()
        self._failure_cleanup_task: asyncio.Task[None] | None = None
        self._startup_task: asyncio.Task[object] | None = None
        self._event_loop_lag_task: asyncio.Task[None] | None = None
        self._late_traffic_finalizations: set[asyncio.Task[None]] = set()

    def capabilities(self) -> CapabilityView:
        available = self.collision_marker.is_file()
        provenance_available = self.build_metadata.available
        return CapabilityView(
            collisionModel="native",
            collisionAvailable=available,
            collisionDetail=(
                "Native SimRadio overlap handling was compiled and probed."
                if available
                else "Native collision marker is absent. Simulation start is disabled."
            ),
            provenanceAvailable=provenance_available,
            firmwareCommit=self.build_metadata.firmware_commit,
            collisionPatchSha256=self.build_metadata.collision_patch_sha256,
            firmwareBinarySha256=self.build_metadata.firmware_binary_sha256,
            buildArchitecture=self.build_metadata.build_architecture,
            clientLibraryVersion=self.build_metadata.client_library_version,
            upstreamBaseImageDigest=self.build_metadata.upstream_base_image_digest,
            meshtasticatorCommit=self.build_metadata.meshtasticator_commit,
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
        await self._await_failure_cleanup()
        async with self._lifecycle_lock:
            if self.state in {LifecycleState.RUNNING, LifecycleState.WARMING_UP}:
                return CommandResult(commandId=command_id, state=self.state, detail="already running")
            if self.state not in {LifecycleState.STOPPED, LifecycleState.FAILED}:
                raise SimulationConflict("INVALID_LIFECYCLE_STATE", f"cannot start from {self.state}")
            if self.state == LifecycleState.FAILED:
                await self._cleanup_resources()
            capabilities = self.capabilities()
            if not capabilities.collision_available:
                raise SimulationConflict(
                    "NATIVE_COLLISION_UNAVAILABLE",
                    "the runtime does not contain a verified collision-enabled native firmware build",
                )
            if not capabilities.provenance_available:
                raise SimulationConflict(
                    "BUILD_METADATA_UNAVAILABLE",
                    "the runtime does not contain build metadata for the native firmware artifact",
                )

            self.simulation_id = str(uuid.uuid4())
            self.state = LifecycleState.STARTING
            self.message = "Starting native firmware processes"
            self._publish_lifecycle()
            self._startup_task = asyncio.current_task()
            try:
                records = await self.supervisor.start(self.scenario)
                self._raise_if_startup_failed()
                self.node_metrics = {
                    node.id: _NodeMetrics(rx_bad_initialized=self.scenario.fresh_state)
                    for node in self.scenario.nodes
                }
                self._local_stats_response_ids.clear()
                self.gateways = {
                    node.id: NodeGateway(
                        node_id=node.id,
                        downstream_host="127.0.0.1",
                        downstream_port=records[node.id].internal_port,
                        public_host="0.0.0.0",
                        public_port=node.api_port,
                        event_handler=self._on_gateway_event,
                        from_radio_handler=self._on_from_radio,
                        public_clients_enabled=False,
                    )
                    for node in self.scenario.nodes
                }
                for gateway in self.gateways.values():
                    gateway.reserve_public_listener()
                gateway_starts = [
                    asyncio.create_task(gateway.start(), name=f"gateway-start-{node_id}")
                    for node_id, gateway in self.gateways.items()
                ]
                try:
                    await asyncio.gather(*gateway_starts)
                except BaseException:
                    for task in gateway_starts:
                        task.cancel()
                    await asyncio.gather(*gateway_starts, return_exceptions=True)
                    raise
                self._raise_if_startup_failed()
                self.message = "Configuring and verifying native nodes"
                self.verifications = await self._configure_nodes()
                self._raise_if_startup_failed()
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
                    build_metadata=self.build_metadata,
                    failed_reception_sampler=self._sample_local_stats,
                )
                self.medium = DirectedMedium(
                    scenario=self.scenario,
                    gateways=self.gateways,
                    event_broker=self.event_broker,
                    hardware_ids=hardware_ids,
                    transmission_handler=self.traffic.record_rf_transmission,
                    drop_handler=self.traffic.record_drop,
                    failure_handler=self._on_medium_failure,
                )
                await self.medium.start()
                self._raise_if_startup_failed()
                self.state = LifecycleState.WARMING_UP
                self.message = "Firmware is exchanging NodeInfo and establishing routes"
                warmup_deadline = self._warmup_deadline_seconds()
                self.warming_up_until = datetime.fromtimestamp(
                    datetime.now(UTC).timestamp() + warmup_deadline + self.warmup_seconds, UTC
                )
                self._publish_lifecycle()
                missing_pairs, expected_pairs = await self._warm_up_nodes()
                if self.state == LifecycleState.FAILED:
                    raise RuntimeError(self.message)
                await asyncio.gather(
                    *(gateway.enable_public_clients() for gateway in self.gateways.values())
                )
                self._raise_if_startup_failed()
                self.state = LifecycleState.RUNNING
                self.message = f"{len(self.gateways)} native nodes are running"
                if missing_pairs:
                    self.message += (
                        f"; warm-up observed {expected_pairs - len(missing_pairs)} of "
                        f"{expected_pairs} graph-connected pairs"
                    )
                self.warming_up_until = None
                self._event_loop_lag_task = asyncio.create_task(
                    self._measure_event_loop_lag(), name="event-loop-lag"
                )
                self._publish_lifecycle()
                return CommandResult(commandId=command_id, state=self.state, detail=self.message)
            except BaseException as exc:
                self.state = LifecycleState.FAILED
                self.message = f"Startup failed: {exc}"
                self._publish_lifecycle()
                await self._cleanup_resources()
                raise
            finally:
                self._startup_task = None

    async def stop(self) -> CommandResult:
        command_id = str(uuid.uuid4())
        await self._await_failure_cleanup()
        async with self._lifecycle_lock:
            if self.state == LifecycleState.STOPPED:
                return CommandResult(commandId=command_id, state=self.state, detail="already stopped")
            self.state = LifecycleState.STOPPING
            self.message = "Stopping traffic, gateways, and native processes"
            self._publish_lifecycle()
            async with self._topology_lock:
                await self._cleanup_resources()
            self.state = LifecycleState.STOPPED
            self.message = "Stopped"
            self.simulation_id = None
            self.warming_up_until = None
            self._publish_lifecycle()
            return CommandResult(commandId=command_id, state=self.state, detail=self.message)

    async def reset(self) -> CommandResult:
        await self._await_failure_cleanup()
        async with self._lifecycle_lock:
            if self.state != LifecycleState.STOPPED:
                raise SimulationConflict("INVALID_LIFECYCLE_STATE", "reset is allowed only while stopped")
            self.scenario = default_scenario()
            self.supervisor.clear_archived_logs()
            self.message = (
                "Scenario reset to the five-node full mesh; packet evidence and daemon logs "
                "were cleared; saved runs were preserved"
            )
            self._publish_lifecycle()
            self.event_broker.clear()
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
        return (await self.update_links([link]))[0]

    async def update_links(self, updates: list[DirectedLink]) -> list[DirectedLink]:
        if not updates:
            raise SimulationConflict("INVALID_LINK_UPDATE", "at least one link update is required")
        update_map = {(link.from_node, link.to_node): link for link in updates}
        if len(update_map) != len(updates):
            raise SimulationConflict("INVALID_LINK_UPDATE", "link updates must be unique")
        async with self._topology_lock:
            if self.state != LifecycleState.RUNNING or self.medium is None:
                raise SimulationConflict("INVALID_LIFECYCLE_STATE", "runtime links require RUNNING state")
            existing = self.scenario.link_map()
            unknown = next((key for key in update_map if key not in existing), None)
            if unknown is not None:
                raise SimulationConflict(
                    "UNKNOWN_LINK",
                    f"unknown directed link: {unknown[0]} -> {unknown[1]}",
                )
            links = [
                update_map.get((current.from_node, current.to_node), current)
                for current in self.scenario.links
            ]
            await self._apply_runtime_scenario(self.scenario.model_copy(update={"links": links}))
            return updates

    async def apply_topology(self, preset: TopologyPreset) -> Scenario:
        async with self._topology_lock:
            updated = apply_topology_preset(self.scenario, preset)
            if self.state == LifecycleState.STOPPED:
                self.scenario = updated
                return updated
            if self.state != LifecycleState.RUNNING or self.medium is None:
                raise SimulationConflict(
                    "INVALID_LIFECYCLE_STATE", f"cannot apply topology from {self.state}"
                )
            await self._apply_runtime_scenario(updated)
            return updated

    async def _apply_runtime_scenario(self, updated: Scenario) -> None:
        if self.medium is None:
            raise RuntimeError("runtime medium is unavailable")
        previous = self.scenario.link_map()
        changed = [
            link for link in updated.links if previous[(link.from_node, link.to_node)] != link
        ]
        try:
            if self.traffic is not None:
                self.traffic.ensure_topology_change_capacity(len(changed))
        except RuntimeError as exc:
            raise SimulationConflict("TOPOLOGY_HISTORY_FULL", str(exc)) from exc
        events = await self.medium.apply_links(updated.links)
        self.scenario = updated
        if self.traffic is not None:
            for event, link in zip(events, changed, strict=True):
                self.traffic.record_topology_change(event, link)

    async def start_traffic(self, request: TrafficRunRequest) -> str:
        async with self._topology_lock:
            if self.state != LifecycleState.RUNNING or self.traffic is None:
                raise SimulationConflict("INVALID_LIFECYCLE_STATE", "traffic requires RUNNING state")
            traffic = self.traffic
            if traffic.state in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}:
                raise SimulationConflict("TRAFFIC_RUN_ACTIVE", "a traffic run is already active")
            try:
                baseline = await self._sample_local_stats()
            except Exception as exc:
                raise SimulationConflict(
                    "LOCAL_STATS_UNAVAILABLE", f"could not capture native local statistics: {exc}"
                ) from exc
            if self.state != LifecycleState.RUNNING or self.traffic is not traffic:
                raise SimulationConflict(
                    "INVALID_LIFECYCLE_STATE",
                    "simulation stopped while traffic baselines were being captured",
                )
            try:
                self._archive_unpersisted_traffic_result()
                return traffic.start(
                    request,
                    scenario_snapshot=self.scenario.model_copy(deep=True),
                    failed_reception_baseline=baseline,
                )
            except PacketIdQuarantineCapacityError as exc:
                raise SimulationConflict("PACKET_ID_QUARANTINE_FULL", str(exc)) from exc
            except RuntimeError as exc:
                raise SimulationConflict("TRAFFIC_RUN_ACTIVE", str(exc)) from exc
            except ValueError as exc:
                raise SimulationConflict("INVALID_TRAFFIC_REQUEST", str(exc)) from exc

    async def stop_traffic(self) -> None:
        async with self._topology_lock:
            if self.traffic is not None:
                await self.traffic.stop()

    def traffic_result(self, run_id: str) -> TrafficRunResult:
        if self.traffic is not None and self.traffic.current is not None:
            if self.traffic.current.run_id == run_id:
                result = self.traffic.result()
                if result is not None:
                    return result
        path = self.results_root / f"{run_id}.json"
        if path.is_file():
            return TrafficRunResult.model_validate_json(path.read_text(encoding="utf-8"))
        volatile = self._volatile_results.get(run_id)
        if volatile is not None:
            return volatile.model_copy(deep=True)
        raise FileNotFoundError(run_id)

    def traffic_result_is_complete(self, run_id: str) -> bool:
        """Check export readiness without copying a run's generated-message records."""

        if self.traffic is not None and self.traffic.current is not None:
            if self.traffic.current.run_id == run_id:
                return self.traffic.result_is_finalized(run_id)
        if (self.results_root / f"{run_id}.json").is_file() or run_id in self._volatile_results:
            return True
        raise FileNotFoundError(run_id)

    def traffic_summary(self, run_id: str) -> TrafficRunSummary:
        if self.traffic is not None and self.traffic.current is not None:
            if self.traffic.current.run_id == run_id:
                summary = self.traffic.summary()
                if summary is not None:
                    return summary
        summary_path = self.results_root / f"{run_id}.summary.json"
        if summary_path.is_file():
            return TrafficRunSummary.model_validate_json(summary_path.read_text(encoding="utf-8"))
        return summarize_result(self.traffic_result(run_id))

    def current_traffic_summary(self) -> TrafficRunSummary | None:
        if self.traffic is not None:
            return self.traffic.summary()
        return self._last_traffic_summary

    def completed_runs(self) -> list[str]:
        persisted = (
            {
                path.stem
                for path in self.results_root.glob("*.json")
                if not path.name.endswith(".summary.json")
            }
            if self.results_root.is_dir()
            else set()
        )
        return sorted(persisted | self._volatile_results.keys())

    def _archive_unpersisted_traffic_result(
        self, traffic: TrafficController | None = None
    ) -> None:
        selected = traffic or self.traffic
        if selected is None or selected.current is None:
            return
        run_id = selected.current.run_id
        if not selected.result_is_finalized(run_id):
            return
        if (self.results_root / f"{run_id}.json").is_file():
            return
        result = selected.result()
        if result is None or result.finished_at is None:
            return
        self._volatile_results[result.run_id] = result
        while len(self._volatile_results) > MAX_VOLATILE_RESULTS:
            self._volatile_results.pop(next(iter(self._volatile_results)))

    def _track_late_traffic_finalization(self, traffic: TrafficController) -> None:
        task = asyncio.create_task(
            self._settle_late_traffic_finalization(traffic),
            name=f"late-traffic-finalization-{traffic.current.run_id if traffic.current else 'unknown'}",
        )
        self._late_traffic_finalizations.add(task)
        task.add_done_callback(self._late_traffic_finalizations.discard)

    async def _settle_late_traffic_finalization(self, traffic: TrafficController) -> None:
        await traffic.wait_for_finalization()
        summary = traffic.summary()
        if summary is not None and (
            self._last_traffic_summary is None
            or self._last_traffic_summary.run_id == summary.run_id
        ):
            self._last_traffic_summary = summary
        self._archive_unpersisted_traffic_result(traffic)

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
        async def settle_node_tasks(
            tasks: list[asyncio.Task[_TaskResult]],
        ) -> list[_TaskResult]:
            try:
                return await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            except Exception:
                # Official-client calls run in worker threads and cannot be
                # cancelled safely. Let every bounded sibling finish before
                # startup cleanup closes gateways and process state.
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

        async def configure(node_id: str) -> NodeVerification:
            node = next(candidate for candidate in self.scenario.nodes if candidate.id == node_id)
            verification = await asyncio.to_thread(
                configure_and_verify_node,
                hostname=self.gateways[node_id].control_host,
                port=self.gateways[node_id].control_port,
                node=node,
                rf=self.scenario.rf,
                channel=self.scenario.channel,
                reboot_after_apply=True,
            )
            await asyncio.wait_for(self.gateways[node_id].client_disconnected.wait(), timeout=5)
            return verification

        configure_tasks = [
            asyncio.create_task(configure(node.id), name=f"configure-{node.id}")
            for node in self.scenario.nodes
        ]
        await settle_node_tasks(configure_tasks)

        async def wait_for_reboot(node_id: str) -> str:
            gateway = self.gateways[node_id]
            await asyncio.wait_for(gateway.failed.wait(), timeout=12)
            return node_id

        pending = await settle_node_tasks(
            [
                asyncio.create_task(wait_for_reboot(node.id), name=f"wait-for-reboot-{node.id}")
                for node in self.scenario.nodes
            ]
        )
        verified: dict[str, NodeVerification] = {}

        async def verify_restarted(node_id: str) -> tuple[str, NodeVerification | None]:
            gateway = self.gateways[node_id]
            node = next(candidate for candidate in self.scenario.nodes if candidate.id == node_id)
            verification = await asyncio.to_thread(
                verify_node,
                hostname=gateway.control_host,
                port=gateway.control_port,
                node=node,
                rf=self.scenario.rf,
                channel=self.scenario.channel,
            )
            await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)
            try:
                await asyncio.wait_for(gateway.failed.wait(), timeout=11)
            except TimeoutError:
                return node_id, verification
            return node_id, None

        for _attempt in range(3):
            await settle_node_tasks(
                [
                    asyncio.create_task(self.gateways[node_id].stop(), name=f"stop-{node_id}")
                    for node_id in pending
                ]
            )
            for node_id in pending:
                self.gateways[node_id].reserve_public_listener()
            await settle_node_tasks(
                [
                    asyncio.create_task(self.gateways[node_id].start(), name=f"start-{node_id}")
                    for node_id in pending
                ]
            )
            observations = await settle_node_tasks(
                [
                    asyncio.create_task(
                        verify_restarted(node_id), name=f"verify-restarted-{node_id}"
                    )
                    for node_id in pending
                ]
            )
            pending = []
            for node_id, verification in observations:
                if verification is None:
                    pending.append(node_id)
                else:
                    verified[node_id] = verification
            if not pending:
                return verified
        raise RuntimeError(f"{', '.join(pending)} did not stabilize after configuration restarts")

    async def _on_gateway_event(self, event: GatewayEvent) -> None:
        if event.kind in {
            "gateway.started",
            "gateway.stopped",
            "gateway.failed",
            "gateway.client_connected",
            "gateway.client_disconnected",
        }:
            self.event_broker.publish(
                PacketEvent(
                    monotonicSeconds=time.monotonic(),
                    eventType=EventType.NODE_STATE,
                    transmitter=event.node_id,
                    result=event.kind,
                    detail=event.detail,
                )
            )
        if event.kind in {"gateway.failed", "gateway.rf_queue_full"} and self.state in {
            LifecycleState.WARMING_UP,
            LifecycleState.RUNNING,
        }:
            category = (
                "SIMULATOR_OVERLOAD"
                if event.kind == "gateway.rf_queue_full"
                else "GATEWAY_FAILED"
            )
            self._schedule_runtime_failure(
                category, f"Gateway for {event.node_id}: {event.detail}"
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
                rx_bad_initialized = metrics.rx_bad_initialized
                metrics.tx = telemetry.local_stats.num_packets_tx
                metrics.rx = telemetry.local_stats.num_packets_rx
                metrics.rx_bad = telemetry.local_stats.num_packets_rx_bad
                metrics.rx_bad_initialized = True
                metrics.rx_duplicate = telemetry.local_stats.num_rx_dupe
                if self.traffic is not None:
                    self.traffic.record_failed_receptions(
                        node_id, telemetry.local_stats.num_packets_rx_bad
                    )
                    self.traffic.record_duplicate_receptions(
                        node_id, telemetry.local_stats.num_rx_dupe
                    )
                if rx_bad_initialized and telemetry.local_stats.num_packets_rx_bad > previous_bad:
                    self.event_broker.publish(
                        PacketEvent(
                            monotonicSeconds=time.monotonic(),
                            eventType=EventType.COLLISION,
                            receiver=node_id,
                            result="native-rx-bad",
                            detail="Firmware local statistics incremented num_packets_rx_bad",
                        )
                    )
                async with self._local_stats_condition:
                    self._local_stats_response_ids[node_id] = packet.decoded.request_id
                    self._local_stats_condition.notify_all()
            elif variant == "device_metrics":
                metrics.channel_utilization = telemetry.device_metrics.channel_utilization

    async def _sample_local_stats(self) -> FailedReceptionSample:
        node_ids = list(self.gateways)
        if not node_ids or set(node_ids) != set(self.verifications):
            raise RuntimeError("native nodes are not ready for local-stat sampling")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.local_stats_deadline_seconds

        async def sample_node(node_id: str) -> tuple[int, int] | None:
            expected_packet_ids: set[int] = set()

            def response_arrived() -> bool:
                response_id = self._local_stats_response_ids.get(node_id)
                return response_id is not None and response_id in expected_packet_ids

            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return None
                request = self._local_stats_request(node_id)
                try:
                    await self.gateways[node_id].send_to_radio(
                        request, source="controller.local-stats"
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.warning(
                        "native local-stat request failed",
                        exc_info=True,
                        extra={"node_id": node_id},
                    )
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        return None
                    await asyncio.sleep(min(self.local_stats_retry_seconds, remaining))
                else:
                    expected_packet_ids.add(request.packet.id)
                    wait_seconds = min(self.local_stats_retry_seconds, deadline - loop.time())
                    if wait_seconds <= 0:
                        return None
                    try:
                        async with self._local_stats_condition:
                            await asyncio.wait_for(
                                self._local_stats_condition.wait_for(response_arrived),
                                timeout=wait_seconds,
                            )
                        metrics = self.node_metrics[node_id]
                        return metrics.rx_bad, metrics.rx_duplicate
                    except TimeoutError:
                        pass

        values = await asyncio.gather(*(sample_node(node_id) for node_id in node_ids))
        totals = {
            node_id: value[0]
            for node_id, value in zip(node_ids, values, strict=True)
            if value is not None
        }
        duplicate_totals = {
            node_id: value[1]
            for node_id, value in zip(node_ids, values, strict=True)
            if value is not None
        }
        missing = tuple(node_id for node_id in node_ids if node_id not in totals)
        return FailedReceptionSample(
            totals=totals, duplicate_totals=duplicate_totals, missing_nodes=missing
        )

    def _local_stats_request(self, node_id: str) -> mesh_pb2.ToRadio:
        telemetry = telemetry_pb2.Telemetry()
        telemetry.local_stats.CopyFrom(telemetry_pb2.LocalStats())
        request = mesh_pb2.ToRadio()
        request.packet.id = self._next_control_packet_id()
        request.packet.to = self.verifications[node_id].node_number
        request.packet.priority = mesh_pb2.MeshPacket.Priority.RELIABLE
        request.packet.decoded.portnum = portnums_pb2.TELEMETRY_APP
        request.packet.decoded.payload = telemetry.SerializeToString()
        request.packet.decoded.want_response = True
        return request

    def _next_control_packet_id(self) -> int:
        self._control_packet_id = (self._control_packet_id + 1) & 0xFFFFFFFF
        if self._control_packet_id == 0:
            self._control_packet_id = 1
        return self._control_packet_id

    async def _warm_up_nodes(self) -> tuple[set[tuple[str, str]], int]:
        expected = self.scenario.reachable_pairs(max_hops=self.scenario.rf.hop_limit)
        self.nodeinfo_observations.clear()
        deadline_seconds = self._warmup_deadline_seconds()

        async def request_once(index: int, node_id: str) -> str | None:
            await asyncio.sleep(index * 0.15)
            gateway = self.gateways[node_id]
            try:
                await asyncio.to_thread(
                    request_node_info,
                    hostname=gateway.control_host,
                    port=gateway.control_port,
                    deadline_seconds=deadline_seconds,
                )
                await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)
            except Exception as exc:
                return f"{node_id}: {exc}"
            return None

        errors = await asyncio.gather(
            *(request_once(index, node.id) for index, node in enumerate(self.scenario.nodes))
        )
        for error in errors:
            if error is not None:
                LOGGER.warning("best-effort NodeInfo warm-up failed", extra={"detail": error})
        await asyncio.sleep(self.warmup_seconds)
        missing = expected - self.nodeinfo_observations
        if missing:
            LOGGER.warning(
                "NodeInfo warm-up ended with missing observations",
                extra={"missing_pair_count": len(missing), "expected_pair_count": len(expected)},
            )
        return missing, len(expected)

    def _warmup_deadline_seconds(self) -> float:
        return min(30, 10 + 2 * len(self.scenario.nodes))

    async def _on_medium_failure(self, node_id: str, exc: Exception) -> None:
        self._schedule_runtime_failure(
            "SIMULATOR_OVERLOAD", f"RF medium worker for {node_id} failed: {exc}"
        )

    async def _on_process_failure(self, node_id: str, return_code: int | None) -> None:
        if self.state in {LifecycleState.STOPPING, LifecycleState.STOPPED, LifecycleState.FAILED}:
            return
        self._schedule_runtime_failure(
            "FIRMWARE_PROCESS_FAILED", f"Firmware node {node_id} exited with code {return_code}"
        )

    def _schedule_runtime_failure(self, category: str, detail: str) -> None:
        if self.state in {LifecycleState.STOPPING, LifecycleState.STOPPED, LifecycleState.FAILED}:
            return
        self.state = LifecycleState.FAILED
        self.message = f"{category}: {detail}"
        self._publish_lifecycle()
        if self._startup_task is not None:
            return
        if self._failure_cleanup_task is None or self._failure_cleanup_task.done():
            self._failure_cleanup_task = asyncio.create_task(
                self._fail_traffic_and_cleanup(self.message), name="failed-simulation-cleanup"
            )

    async def _fail_traffic_and_cleanup(self, reason: str) -> None:
        async with self._lifecycle_lock:
            async with self._topology_lock:
                async with self._cleanup_lock:
                    if self.traffic is not None:
                        await self.traffic.fail(reason)
                    await self._cleanup_resources_unlocked()

    def _raise_if_startup_failed(self) -> None:
        if self.state == LifecycleState.FAILED:
            raise RuntimeError(self.message)

    async def _await_failure_cleanup(self) -> bool:
        task = self._failure_cleanup_task
        if task is None or task is asyncio.current_task():
            return False
        try:
            await asyncio.shield(task)
        except Exception:
            LOGGER.exception("failed simulation cleanup did not complete")
            return False
        finally:
            if self._failure_cleanup_task is task:
                self._failure_cleanup_task = None
        return True

    async def _cleanup_resources(self) -> None:
        async with self._cleanup_lock:
            await self._cleanup_resources_unlocked()

    async def _cleanup_resources_unlocked(self) -> None:
        lag_task, self._event_loop_lag_task = self._event_loop_lag_task, None
        if lag_task is not None:
            lag_task.cancel()
            await asyncio.gather(lag_task, return_exceptions=True)
        if self.traffic is not None:
            traffic = self.traffic
            finalized = await traffic.stop()
            self._last_traffic_summary = traffic.summary()
            if finalized:
                self._archive_unpersisted_traffic_result(traffic)
            else:
                self._track_late_traffic_finalization(traffic)
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

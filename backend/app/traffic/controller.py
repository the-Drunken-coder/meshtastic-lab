"""Fixed-rate traffic through the same ToRadio path as an external client."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.gateway import NodeGateway
from backend.app.metrics import (
    EventBroker,
    EventType,
    MetricsSnapshot,
    MetricsSummary,
    PacketEvent,
    calculate_metrics,
)
from backend.app.models import DirectedLink, Scenario
from backend.app.provenance import BuildMetadata

TRAFFIC_PREFIX = "ML1"
PACKET_ID_QUARANTINE_SECONDS = 5 * 60
MAX_QUARANTINED_PACKET_IDS_PER_SOURCE = 600 * 60
MAX_TOPOLOGY_CHANGES_PER_RUN = 10_000
PACKET_ID_RNG_SALT = 0x4D4C5F5041434B4554
LOGGER = logging.getLogger(__name__)
FailedReceptionSampler = Callable[[], Awaitable[Mapping[str, int]]]


class PacketIdQuarantineCapacityError(RuntimeError):
    pass


class TrafficKind(StrEnum):
    BROADCAST_TEXT = "broadcast-text"
    DIRECT_TEXT = "direct-text"


class DestinationStrategy(StrEnum):
    FIXED = "fixed"
    ROUND_ROBIN = "round-robin"
    DETERMINISTIC_RANDOM = "deterministic-random"


class TrafficRunState(StrEnum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class TrafficRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: TrafficKind = TrafficKind.BROADCAST_TEXT
    source_nodes: list[str] = Field(alias="sourceNodes", min_length=1)
    destination_strategy: DestinationStrategy = Field(
        default=DestinationStrategy.FIXED, alias="destinationStrategy"
    )
    fixed_destination: str | None = Field(default=None, alias="fixedDestination")
    messages_per_minute: Annotated[float, Field(gt=0, le=600)] = Field(
        default=6, alias="messagesPerMinute"
    )
    payload_bytes: Annotated[int, Field(ge=16, le=233)] = Field(default=64, alias="payloadBytes")
    duration_seconds: Annotated[float, Field(gt=0, le=3600)] = Field(
        default=10, alias="durationSeconds"
    )
    acknowledgment_requested: bool = Field(default=True, alias="acknowledgmentRequested")
    seed: int = 1

    @model_validator(mode="after")
    def validate_destination(self) -> TrafficRunRequest:
        if self.kind == TrafficKind.DIRECT_TEXT:
            if self.destination_strategy == DestinationStrategy.FIXED and self.fixed_destination is None:
                raise ValueError("fixedDestination is required for fixed direct traffic")
        return self


class GeneratedMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: int
    source_node: str = Field(alias="sourceNode")
    destination_node: str = Field(alias="destinationNode")
    packet_id: int = Field(alias="packetId")
    generated_monotonic: float = Field(alias="generatedMonotonic")
    submitted: bool
    submission_error: str | None = Field(default=None, alias="submissionError")
    transmitted: bool = False
    delivered_to: list[str] = Field(default_factory=list, alias="deliveredTo")
    acknowledged: bool = False


class TopologyChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_sequence: int = Field(alias="eventSequence")
    monotonic_seconds: float = Field(alias="monotonicSeconds")
    link: DirectedLink


class _ProvenanceFields(BaseModel):
    model_config = ConfigDict(extra="forbid")

    firmware_commit: str = Field(alias="firmwareCommit")
    collision_patch_sha256: str = Field(alias="collisionPatchSha256")
    firmware_binary_sha256: str = Field(alias="firmwareBinarySha256")
    build_architecture: str = Field(alias="buildArchitecture")
    upstream_base_image_digest: str = Field(alias="upstreamBaseImageDigest")
    meshtasticator_commit: str = Field(alias="meshtasticatorCommit")
    client_library_version: str = Field(alias="clientLibraryVersion")
    collision_model: Literal["native"] = Field(default="native", alias="collisionModel")

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_provenance(cls, values: object) -> object:
        """Keep pre-provenance result files readable without inventing artifact identity."""

        if not isinstance(values, Mapping) or "firmwareImageDigest" not in values:
            return values
        migrated = dict(values)
        legacy_digest = migrated.pop("firmwareImageDigest")
        migrated.setdefault("collisionPatchSha256", "unavailable")
        migrated.setdefault("firmwareBinarySha256", "unavailable")
        migrated.setdefault("buildArchitecture", "unavailable")
        migrated.setdefault("upstreamBaseImageDigest", legacy_digest)
        return migrated


class TrafficRunResult(_ProvenanceFields):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(alias="runId")
    state: TrafficRunState
    request: TrafficRunRequest
    scenario_snapshot: dict[str, object] = Field(alias="scenarioSnapshot")
    started_at: datetime = Field(alias="startedAt")
    finished_at: datetime | None = Field(default=None, alias="finishedAt")
    random_seed: int = Field(alias="randomSeed")
    requested: int = 0
    submitted: int = 0
    submission_failed: int = Field(default=0, alias="submissionFailed")
    transmitted: int = 0
    delivered: int = 0
    generated_messages: list[GeneratedMessage] = Field(default_factory=list, alias="generatedMessages")
    topology_changes: list[TopologyChange] = Field(default_factory=list, alias="topologyChanges")
    metrics: MetricsSnapshot
    failure: str | None = None


class TrafficRunSummary(_ProvenanceFields):
    """Bounded status for the live endpoint; generated records are export-only."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(alias="runId")
    state: TrafficRunState
    request: TrafficRunRequest
    scenario_snapshot: dict[str, object] = Field(alias="scenarioSnapshot")
    started_at: datetime = Field(alias="startedAt")
    finished_at: datetime | None = Field(default=None, alias="finishedAt")
    random_seed: int = Field(alias="randomSeed")
    requested: int = 0
    submitted: int = 0
    submission_failed: int = Field(default=0, alias="submissionFailed")
    transmitted: int = 0
    delivered: int = 0
    metrics: MetricsSummary
    failure: str | None = None


def summarize_result(result: TrafficRunResult) -> TrafficRunSummary:
    """Strip export-only records and per-message metrics from a terminal result."""

    values = result.model_dump(
        mode="python", by_alias=True, exclude={"generated_messages", "topology_changes"}
    )
    metrics = values["metrics"]
    if not isinstance(metrics, dict):
        raise TypeError("traffic result metrics did not serialize as an object")
    metrics.pop("receiversPerBroadcast", None)
    return TrafficRunSummary.model_validate(values)


class TrafficController:
    """Own at most one run and keep metrics independent of the UI event buffer."""

    def __init__(
        self,
        *,
        scenario: Scenario,
        gateways: Mapping[str, NodeGateway],
        hardware_ids: Mapping[str, int],
        event_broker: EventBroker,
        results_root: Path,
        build_metadata: BuildMetadata | None = None,
        settle_seconds: float = 3.0,
        failed_reception_sampler: FailedReceptionSampler | None = None,
    ) -> None:
        self.scenario = scenario
        self.gateways = gateways
        self.hardware_ids = hardware_ids
        self.event_broker = event_broker
        self.results_root = results_root
        self.build_metadata = build_metadata or BuildMetadata.unavailable()
        self.settle_seconds = settle_seconds
        self.failed_reception_sampler = failed_reception_sampler
        self.state = TrafficRunState.IDLE
        self.current: TrafficRunResult | None = None
        self._frozen_result: TrafficRunResult | None = None
        self._task: asyncio.Task[None] | None = None
        self._run_scenario = scenario
        self._sequence = 0
        self._messages_by_packet: dict[tuple[int, int], GeneratedMessage] = {}
        self._messages_by_key: dict[tuple[str, int], GeneratedMessage] = {}
        self._packet_ids_by_source: dict[str, dict[int, float]] = {}
        self._packet_id_quarantine: dict[str, deque[tuple[float, int]]] = {}
        self._quarantined_packet_ids: dict[str, set[int]] = {}
        self._delivered_sequences: set[int] = set()
        self._latencies_ms: list[float] = []
        self._rf_transmitters: list[str] = []
        self._airtimes_ms: list[int] = []
        self._relay_transmissions = 0
        self._duplicates = 0
        self._failed_receptions = 0
        self._latest_failed_receptions: dict[str, int] = {}
        self._drop_reasons: list[str] = []
        self._drop_counts: Counter[str] = Counter()
        self._event_loop_lag_ms: float | None = None
        self._generated_count = 0
        self._submitted_count = 0
        self._submission_failed_count = 0
        self._transmitted_count = 0
        self._unique_deliveries = 0
        self._receiver_deliveries = 0
        self._receiver_opportunities = 0
        self._acknowledgments = 0
        self._rf_transmission_count = 0
        self._observed_airtime_ms = 0
        self._per_node_transmit_counts: Counter[str] = Counter()
        self._metrics: MetricsSnapshot | None = None

    def start(
        self,
        request: TrafficRunRequest,
        *,
        scenario_snapshot: Scenario | None = None,
        failed_reception_baseline: Mapping[str, int] | None = None,
    ) -> str:
        if self._task is not None and not self._task.done():
            raise RuntimeError("a traffic run is already active")
        self._validate_request_nodes(request, scenario_snapshot=scenario_snapshot)
        run_id = str(uuid.uuid4())
        snapshot = (scenario_snapshot or self.scenario).model_copy(deep=True)
        max_sequence = self._maximum_sequence(request)
        marker_size = len(f"{TRAFFIC_PREFIX}:{run_id}:{max_sequence}:")
        if request.payload_bytes < marker_size:
            raise ValueError(
                f"payloadBytes {request.payload_bytes} is smaller than encoded traffic identifier "
                f"{marker_size} for sequence {max_sequence}"
            )
        self._reset_accumulators()
        self._latest_failed_receptions.update(failed_reception_baseline or {})
        self._run_scenario = snapshot
        self.current = TrafficRunResult(
            runId=run_id,
            state=TrafficRunState.RUNNING,
            request=request,
            scenarioSnapshot=snapshot.model_dump(by_alias=True),
            firmwareCommit=self.build_metadata.firmware_commit,
            collisionPatchSha256=self.build_metadata.collision_patch_sha256,
            firmwareBinarySha256=self.build_metadata.firmware_binary_sha256,
            buildArchitecture=self.build_metadata.build_architecture,
            upstreamBaseImageDigest=self.build_metadata.upstream_base_image_digest,
            meshtasticatorCommit=self.build_metadata.meshtasticator_commit,
            clientLibraryVersion=self.build_metadata.client_library_version,
            startedAt=datetime.now(UTC),
            randomSeed=request.seed,
            metrics=self._live_metrics_snapshot(),
        )
        self._frozen_result = None
        self.state = TrafficRunState.RUNNING
        self._task = asyncio.create_task(self._run(), name=f"traffic-{run_id}")
        LOGGER.info("traffic run started", extra={"traffic_run_id": run_id})
        return run_id

    async def stop(self) -> None:
        if self._frozen_result is not None:
            return
        if self.state == TrafficRunState.FAILED:
            await self._cancel_run_task()
            await self._sample_final_failed_receptions(required=False)
            if self._frozen_result is None:
                await self._finish(TrafficRunState.FAILED)
            return
        if self._task is None or self._task.done():
            return
        self.state = TrafficRunState.STOPPING
        await self._cancel_run_task()
        try:
            await self._sample_final_failed_receptions(required=True)
        except RuntimeError as exc:
            self.state = TrafficRunState.FAILED
            if self.current is not None:
                self.current.failure = str(exc)
        if self._frozen_result is None:
            terminal = (
                TrafficRunState.FAILED
                if self.state == TrafficRunState.FAILED
                else TrafficRunState.CANCELLED
            )
            await self._finish(terminal)

    async def fail(self, reason: str) -> None:
        """Fail an active run, preserving FAILED when its task observes cancellation."""

        if self.current is None or self.state not in {
            TrafficRunState.RUNNING,
            TrafficRunState.STOPPING,
            TrafficRunState.FAILED,
        }:
            return
        self.state = TrafficRunState.FAILED
        self.current.failure = reason
        await self._cancel_run_task()
        await self._sample_final_failed_receptions(required=False)
        if self._frozen_result is None:
            await self._finish(TrafficRunState.FAILED)

    async def wait(self, *, deadline_seconds: float | None = None) -> TrafficRunResult:
        if self._task is not None:
            if deadline_seconds is None:
                await self._task
            else:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=deadline_seconds)
        result = self.result()
        if result is None:
            raise RuntimeError("no traffic run exists")
        return result

    def summary(self) -> TrafficRunSummary | None:
        if self.current is None:
            return None
        result = self._frozen_result or self.current
        if self._frozen_result is not None:
            return summarize_result(result)
        metrics = self._live_metrics()
        return TrafficRunSummary(
            runId=result.run_id,
            state=result.state,
            request=result.request,
            scenarioSnapshot=result.scenario_snapshot,
            firmwareCommit=result.firmware_commit,
            collisionPatchSha256=result.collision_patch_sha256,
            firmwareBinarySha256=result.firmware_binary_sha256,
            buildArchitecture=result.build_architecture,
            upstreamBaseImageDigest=result.upstream_base_image_digest,
            meshtasticatorCommit=result.meshtasticator_commit,
            clientLibraryVersion=result.client_library_version,
            startedAt=result.started_at,
            finishedAt=result.finished_at,
            randomSeed=result.random_seed,
            requested=self._generated_count if self._frozen_result is None else result.requested,
            submitted=self._submitted_count if self._frozen_result is None else result.submitted,
            submissionFailed=(
                self._submission_failed_count if self._frozen_result is None else result.submission_failed
            ),
            transmitted=self._transmitted_count if self._frozen_result is None else result.transmitted,
            delivered=self._unique_deliveries if self._frozen_result is None else result.delivered,
            metrics=metrics,
            failure=result.failure,
        )

    def result(self) -> TrafficRunResult | None:
        if self._frozen_result is not None:
            return self._frozen_result.model_copy(deep=True)
        if self.current is None:
            return None
        return self.current.model_copy(deep=True)

    def snapshot(self) -> TrafficRunResult | None:
        """Return the full result for compatibility; live endpoints should use summary()."""

        return self.result()

    def record_rf_transmission(
        self, transmitter: str, packet: mesh_pb2.MeshPacket, packet_airtime_ms: int
    ) -> None:
        if self.current is None or self.state not in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}:
            return
        message = self._message_for_packet_identity(packet)
        if message is None:
            return
        origin = self._packet_origin(packet)
        if not message.transmitted:
            message.transmitted = True
            self._transmitted_count += 1
        self._rf_transmitters.append(transmitter)
        self._airtimes_ms.append(packet_airtime_ms)
        self._rf_transmission_count += 1
        self._observed_airtime_ms += packet_airtime_ms
        self._per_node_transmit_counts[transmitter] += 1
        if origin != self.hardware_ids[transmitter]:
            self._relay_transmissions += 1

    def record_drop(self, transmitter: str, packet: mesh_pb2.MeshPacket, reason: str) -> None:
        del transmitter
        if self.current is None or self.state not in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}:
            return
        if self._message_for_packet_identity(packet) is None:
            return
        self._drop_reasons.append(reason)
        self._drop_counts[reason] += 1

    def record_failed_receptions(self, node_id: str, total: int) -> None:
        if self.current is None or self.state not in {
            TrafficRunState.RUNNING,
            TrafficRunState.STOPPING,
            TrafficRunState.FAILED,
        }:
            return
        previous = self._latest_failed_receptions.get(node_id)
        self._latest_failed_receptions[node_id] = total
        if previous is None:
            return
        self._failed_receptions += max(0, total - previous)

    def ensure_topology_change_capacity(self, additional_changes: int) -> None:
        if self.current is None or self.state not in {
            TrafficRunState.RUNNING,
            TrafficRunState.STOPPING,
        }:
            return
        if len(self.current.topology_changes) + additional_changes > MAX_TOPOLOGY_CHANGES_PER_RUN:
            raise RuntimeError(
                f"traffic run cannot record more than {MAX_TOPOLOGY_CHANGES_PER_RUN} topology changes"
            )

    def record_topology_change(self, event: PacketEvent, link: DirectedLink) -> None:
        if self.current is None or self.state not in {
            TrafficRunState.RUNNING,
            TrafficRunState.STOPPING,
        }:
            return
        self.ensure_topology_change_capacity(1)
        self.current.topology_changes.append(
            TopologyChange(
                eventSequence=event.sequence,
                monotonicSeconds=event.monotonic_seconds,
                link=link,
            )
        )

    def set_event_loop_lag(self, lag_ms: float) -> None:
        if self.current is not None and self.state in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}:
            self._event_loop_lag_ms = lag_ms

    async def handle_from_radio(self, node_id: str, message: mesh_pb2.FromRadio) -> None:
        if (
            self.current is None
            or self.state not in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}
            or message.WhichOneof("payload_variant") != "packet"
        ):
            return
        packet = message.packet
        if packet.WhichOneof("payload_variant") != "decoded":
            return
        if packet.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
            generated = self._correlated_delivery_message(packet)
            if generated is None:
                return
            is_applicable_receiver = (
                node_id == generated.destination_node
                if self.current.request.kind == TrafficKind.DIRECT_TEXT
                else node_id != generated.source_node
            )
            if not is_applicable_receiver:
                return
            sequence = generated.sequence
            if node_id in generated.delivered_to:
                self._duplicates += 1
                return
            generated.delivered_to.append(node_id)
            self._receiver_deliveries += 1
            if sequence not in self._delivered_sequences:
                self._delivered_sequences.add(sequence)
                self._unique_deliveries += 1
            self._latencies_ms.append((time.monotonic() - generated.generated_monotonic) * 1000)
            self.event_broker.publish(
                PacketEvent(
                    monotonicSeconds=time.monotonic(),
                    eventType=EventType.APPLICATION_RECEIVE,
                    transmitter=generated.source_node,
                    intendedDestination=generated.destination_node,
                    receiver=node_id,
                    meshPacketId=packet.id,
                    trafficRunId=self.current.run_id,
                    trafficSequence=sequence,
                    result="delivered",
                )
            )
        elif packet.decoded.portnum == portnums_pb2.ROUTING_APP and packet.decoded.request_id:
            acknowledgment_origin = self.hardware_ids.get(node_id)
            if acknowledgment_origin is None:
                return
            generated = self._messages_by_packet.get(
                (acknowledgment_origin, packet.decoded.request_id)
            )
            if generated is None or not generated.submitted or not generated.transmitted:
                return
            routing = mesh_pb2.Routing()
            routing.ParseFromString(packet.decoded.payload)
            if routing.error_reason == mesh_pb2.Routing.Error.NONE:
                response_origin = self._packet_origin(packet)
                if self.current.request.kind == TrafficKind.DIRECT_TEXT:
                    if response_origin != self.hardware_ids[generated.destination_node]:
                        return
                elif response_origin == acknowledgment_origin:
                    return
                if not generated.acknowledged:
                    generated.acknowledged = True
                    self._acknowledgments += 1
                event_type = EventType.ACKNOWLEDGMENT
                result = "acknowledged"
            else:
                event_type = EventType.ROUTING_ERROR
                result = mesh_pb2.Routing.Error.Name(routing.error_reason)
                self._drop_reasons.append(result)
                self._drop_counts[result] += 1
            self.event_broker.publish(
                PacketEvent(
                    monotonicSeconds=time.monotonic(),
                    eventType=event_type,
                    receiver=node_id,
                    meshPacketId=packet.decoded.request_id,
                    trafficRunId=self.current.run_id,
                    trafficSequence=generated.sequence,
                    result=result,
                )
            )

    async def _run(self) -> None:
        if self.current is None:
            return
        request = self.current.request
        destination_randomizer = random.Random(request.seed)
        packet_id_randomizer = random.Random(request.seed ^ PACKET_ID_RNG_SALT)
        interval = 60 / request.messages_per_minute
        started = time.monotonic()
        messages_per_source = self._messages_per_source(request)
        next_tick = {source: 0 for source in request.source_nodes}
        round_robin = {source: 0 for source in request.source_nodes}
        try:
            while next_tick:
                source = min(next_tick, key=next_tick.__getitem__)
                tick = next_tick[source]
                scheduled = started + tick * interval
                await asyncio.sleep(max(0, scheduled - time.monotonic()))
                destination = self._destination_for(
                    request,
                    source,
                    randomizer=destination_randomizer,
                    round_robin=round_robin,
                )
                await self._submit(source, destination, packet_id_randomizer)
                if tick + 1 == messages_per_source:
                    del next_tick[source]
                else:
                    next_tick[source] = tick + 1
            await asyncio.sleep(self.settle_seconds)
            await self._sample_final_failed_receptions(required=True)
            self.state = TrafficRunState.COMPLETED
            await self._finish(TrafficRunState.COMPLETED)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.state = TrafficRunState.FAILED
            if self.current is not None:
                self.current.failure = str(exc)
            await self._finish(TrafficRunState.FAILED)

    async def _submit(self, source: str, destination: str, randomizer: random.Random) -> None:
        if self.current is None:
            return
        self._sequence += 1
        marker = f"{TRAFFIC_PREFIX}:{self.current.run_id}:{self._sequence}:".encode()
        requested_size = self.current.request.payload_bytes
        if len(marker) > requested_size:
            raise ValueError(
                f"payloadBytes {requested_size} is smaller than encoded traffic identifier {len(marker)}"
            )
        payload = marker + b"x" * (requested_size - len(marker))
        packet_id = self._allocate_packet_id(source, randomizer)
        packet = mesh_pb2.MeshPacket(
            id=packet_id,
            to=0xFFFFFFFF if destination == "broadcast" else self.hardware_ids[destination],
            want_ack=self.current.request.acknowledgment_requested,
            hop_limit=self._run_scenario.rf.hop_limit,
            priority=mesh_pb2.MeshPacket.Priority.RELIABLE,
        )
        packet.decoded.portnum = portnums_pb2.TEXT_MESSAGE_APP
        packet.decoded.payload = payload
        generated = GeneratedMessage(
            sequence=self._sequence,
            sourceNode=source,
            destinationNode=destination,
            packetId=packet_id,
            generatedMonotonic=time.monotonic(),
            submitted=False,
        )
        self.current.generated_messages.append(generated)
        self._generated_count += 1
        self._receiver_opportunities += (
            len(self._run_scenario.nodes) - 1
            if self.current.request.kind == TrafficKind.BROADCAST_TEXT
            else 1
        )
        self.current.requested = self._generated_count
        self._messages_by_packet[(self.hardware_ids[source], packet_id)] = generated
        self._messages_by_key[(self.current.run_id, self._sequence)] = generated
        request = mesh_pb2.ToRadio()
        request.packet.CopyFrom(packet)
        try:
            await self.gateways[source].send_to_radio(request, source="traffic")
            generated.submitted = True
            self._submitted_count += 1
            self.current.submitted = self._submitted_count
        except Exception as exc:
            generated.submission_error = str(exc)
            self._submission_failed_count += 1
            self.current.submission_failed = self._submission_failed_count
        self.event_broker.publish(
            PacketEvent(
                monotonicSeconds=generated.generated_monotonic,
                eventType=EventType.TRAFFIC,
                transmitter=source,
                intendedDestination=destination,
                meshPacketId=packet_id,
                trafficRunId=self.current.run_id,
                trafficSequence=self._sequence,
                result="submitted" if generated.submitted else "submission-failed",
            )
        )

    def _destination_for(
        self,
        request: TrafficRunRequest,
        source: str,
        *,
        randomizer: random.Random,
        round_robin: dict[str, int],
    ) -> str:
        if request.kind == TrafficKind.BROADCAST_TEXT:
            return "broadcast"
        candidates = [node.id for node in self._run_scenario.nodes if node.id != source]
        if request.destination_strategy == DestinationStrategy.FIXED:
            if request.fixed_destination is None:
                raise RuntimeError("fixed destination was not validated")
            return request.fixed_destination
        if request.destination_strategy == DestinationStrategy.ROUND_ROBIN:
            index = round_robin[source]
            round_robin[source] += 1
            return candidates[index % len(candidates)]
        return randomizer.choice(candidates)

    async def _finish(self, state: TrafficRunState) -> None:
        if self.current is None or self._frozen_result is not None:
            return
        self.state = state
        current = self.current
        current.state = state
        current.finished_at = datetime.now(UTC)
        current.requested = self._generated_count
        current.submitted = self._submitted_count
        current.submission_failed = self._submission_failed_count
        current.transmitted = self._transmitted_count
        current.delivered = self._unique_deliveries
        self._metrics = self._final_metrics()
        current.metrics = self._metrics
        frozen = current.model_copy(deep=True)
        try:
            self.results_root.mkdir(parents=True, exist_ok=True)
            destination = self.results_root / f"{frozen.run_id}.json"
            temporary = destination.with_suffix(".tmp")
            temporary.write_text(
                json.dumps(frozen.model_dump(mode="json", by_alias=True), indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(destination)
        except Exception as exc:
            self.state = TrafficRunState.FAILED
            current.state = TrafficRunState.FAILED
            persistence_failure = f"result persistence failed: {exc}"
            current.failure = (
                f"{current.failure}; {persistence_failure}"
                if current.failure
                else persistence_failure
            )
            frozen = current.model_copy(deep=True)
            LOGGER.exception(
                "traffic result persistence failed",
                extra={"traffic_run_id": current.run_id},
            )
        self._frozen_result = frozen
        self.current = frozen.model_copy(deep=True)

    def _final_metrics(self) -> MetricsSnapshot:
        delivered_ids: set[tuple[str, int]] = set()
        if self.current is not None:
            if self.current.request.kind == TrafficKind.DIRECT_TEXT:
                delivered_ids = {
                    (self.current.run_id, message.sequence)
                    for message in self.current.generated_messages
                    if message.destination_node in message.delivered_to
                }
            else:
                delivered_ids = {
                    (self.current.run_id, message.sequence)
                    for message in self.current.generated_messages
                    if any(receiver != message.source_node for receiver in message.delivered_to)
                }
        receivers_per_broadcast = (
            {
                str(message.sequence): len(
                    {receiver for receiver in message.delivered_to if receiver != message.source_node}
                )
                for message in (self.current.generated_messages if self.current is not None else [])
            }
            if self.current is not None and self.current.request.kind == TrafficKind.BROADCAST_TEXT
            else {}
        )
        expected_acknowledgments = (
            self._generated_count
            if self.current is not None and self.current.request.acknowledgment_requested
            else 0
        )
        return calculate_metrics(
            generated=self._generated_count,
            delivered_ids=delivered_ids,
            acknowledged=self._acknowledgments,
            acknowledgment_expected=expected_acknowledgments,
            latencies_ms=self._latencies_ms,
            rf_transmitters=self._rf_transmitters,
            relay_transmissions=self._relay_transmissions,
            duplicate_receptions=self._duplicates,
            failed_receptions=self._failed_receptions,
            drop_reasons=self._drop_reasons,
            airtimes_ms=self._airtimes_ms,
            event_loop_lag_ms=self._event_loop_lag_ms,
            receiver_deliveries=self._receiver_deliveries,
            receiver_delivery_opportunities=self._receiver_opportunities,
            receivers_per_broadcast=receivers_per_broadcast,
        )

    def _live_metrics(self) -> MetricsSummary:
        return self._summary_metrics(self._live_metrics_snapshot())

    def _live_metrics_snapshot(self) -> MetricsSnapshot:
        expected_acknowledgments = (
            self._generated_count
            if self.current is not None and self.current.request.acknowledgment_requested
            else 0
        )
        complete = calculate_metrics(
            generated=self._generated_count,
            delivered_ids=set(),
            delivered_count=self._unique_deliveries,
            acknowledged=self._acknowledgments,
            acknowledgment_expected=expected_acknowledgments,
            latencies_ms=[],
            rf_transmitters=[],
            rf_transmission_count=self._rf_transmission_count,
            relay_transmissions=self._relay_transmissions,
            duplicate_receptions=self._duplicates,
            failed_receptions=self._failed_receptions,
            drop_reasons=[],
            drops_by_reason=dict(self._drop_counts),
            airtimes_ms=[],
            observed_airtime_ms=self._observed_airtime_ms,
            per_node_transmit_counts=dict(self._per_node_transmit_counts),
            event_loop_lag_ms=self._event_loop_lag_ms,
            receiver_deliveries=self._receiver_deliveries,
            receiver_delivery_opportunities=self._receiver_opportunities,
            receivers_per_broadcast={},
        )
        return complete

    @staticmethod
    def _summary_metrics(metrics: MetricsSnapshot) -> MetricsSummary:
        values = metrics.model_dump(mode="python", by_alias=True, exclude={"receivers_per_broadcast"})
        return MetricsSummary.model_validate(values)

    def _validate_request_nodes(
        self, request: TrafficRunRequest, *, scenario_snapshot: Scenario | None = None
    ) -> None:
        scenario = scenario_snapshot or self.scenario
        known = {node.id for node in scenario.nodes}
        unknown_sources = set(request.source_nodes) - known
        if unknown_sources:
            raise ValueError(f"unknown traffic sources: {sorted(unknown_sources)}")
        if len(set(request.source_nodes)) != len(request.source_nodes):
            raise ValueError("traffic source nodes must be unique")
        if request.fixed_destination is not None:
            if request.fixed_destination not in known:
                raise ValueError(f"unknown fixed destination: {request.fixed_destination}")
            if request.kind == TrafficKind.DIRECT_TEXT and request.fixed_destination in request.source_nodes:
                raise ValueError("direct traffic destination cannot be one of its source nodes")

    @staticmethod
    def _maximum_sequence(request: TrafficRunRequest) -> int:
        return len(request.source_nodes) * TrafficController._messages_per_source(request)

    @staticmethod
    def _messages_per_source(request: TrafficRunRequest) -> int:
        offered = (
            Decimal(str(request.duration_seconds))
            * Decimal(str(request.messages_per_minute))
            / Decimal(60)
        )
        return max(1, int(offered.to_integral_value(rounding=ROUND_CEILING)))

    def _allocate_packet_id(self, source: str, randomizer: random.Random) -> int:
        now = time.monotonic()
        self._purge_packet_id_quarantine(source, now=now)
        used = self._packet_ids_by_source.setdefault(source, {})
        quarantined = self._quarantined_packet_ids.setdefault(source, set())
        while True:
            packet_id = randomizer.randrange(1, 0xFFFFFFFF)
            if packet_id not in used and packet_id not in quarantined:
                used[packet_id] = now
                return packet_id

    async def _cancel_run_task(self) -> None:
        task = self._task
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _sample_final_failed_receptions(self, *, required: bool) -> None:
        if self.failed_reception_sampler is None:
            return
        try:
            totals = await self.failed_reception_sampler()
        except Exception as exc:
            detail = f"final native local statistics unavailable: {exc}"
            if required:
                raise RuntimeError(detail) from exc
            if self.current is not None:
                self.current.failure = (
                    f"{self.current.failure}; {detail}" if self.current.failure else detail
                )
            return
        for node_id, total in totals.items():
            self.record_failed_receptions(node_id, total)

    def _retire_packet_ids(self) -> None:
        now = time.monotonic()
        for source, packet_ids in self._packet_ids_by_source.items():
            self._purge_packet_id_quarantine(source, now=now)
            quarantine = self._packet_id_quarantine.setdefault(source, deque())
            quarantined = self._quarantined_packet_ids.setdefault(source, set())
            retained = [
                (allocated_at + PACKET_ID_QUARANTINE_SECONDS, packet_id)
                for packet_id, allocated_at in packet_ids.items()
                if allocated_at + PACKET_ID_QUARANTINE_SECONDS > now
            ]
            if len(quarantine) + len(retained) > MAX_QUARANTINED_PACKET_IDS_PER_SOURCE:
                raise PacketIdQuarantineCapacityError(
                    "packet ID quarantine capacity is too small for the configured late-acknowledgment window"
                )
            for expiry, packet_id in retained:
                quarantine.append((expiry, packet_id))
                quarantined.add(packet_id)
        self._packet_ids_by_source.clear()

    def _purge_packet_id_quarantine(self, source: str, *, now: float) -> None:
        quarantine = self._packet_id_quarantine.setdefault(source, deque())
        quarantined = self._quarantined_packet_ids.setdefault(source, set())
        while quarantine and quarantine[0][0] <= now:
            _, packet_id = quarantine.popleft()
            quarantined.discard(packet_id)

    def _message_for_packet_identity(
        self, packet: mesh_pb2.MeshPacket
    ) -> GeneratedMessage | None:
        return self._messages_by_packet.get((self._packet_origin(packet), packet.id))

    def _correlated_delivery_message(
        self, packet: mesh_pb2.MeshPacket
    ) -> GeneratedMessage | None:
        if self.current is None:
            return None
        identifier = self._traffic_identifier_from_packet(packet)
        if identifier is None or identifier[0] != self.current.run_id:
            return None
        message = self._message_for_packet_identity(packet)
        if message is None or message.sequence != identifier[1]:
            return None
        return message

    @classmethod
    def _traffic_identifier_from_packet(
        cls, packet: mesh_pb2.MeshPacket
    ) -> tuple[str, int] | None:
        if packet.WhichOneof("payload_variant") != "decoded":
            return None
        if packet.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
            payload = bytes(packet.decoded.payload)
        elif packet.decoded.portnum == portnums_pb2.SIMULATOR_APP:
            compressed = mesh_pb2.Compressed()
            try:
                compressed.ParseFromString(packet.decoded.payload)
            except Exception:
                return None
            if compressed.portnum != portnums_pb2.TEXT_MESSAGE_APP:
                return None
            payload = bytes(compressed.data)
        else:
            return None
        return cls._parse_identifier(payload)

    @staticmethod
    def _packet_origin(packet: mesh_pb2.MeshPacket) -> int:
        return int(getattr(packet, "from"))

    @staticmethod
    def _parse_identifier(payload: bytes) -> tuple[str, int] | None:
        try:
            prefix, run_id, sequence, _ = payload.decode("utf-8", errors="strict").split(":", 3)
            if prefix != TRAFFIC_PREFIX:
                return None
            return run_id, int(sequence)
        except (ValueError, UnicodeDecodeError):
            return None

    def _reset_accumulators(self) -> None:
        self._retire_packet_ids()
        self._sequence = 0
        self._messages_by_packet.clear()
        self._messages_by_key.clear()
        self._delivered_sequences.clear()
        self._latencies_ms.clear()
        self._rf_transmitters.clear()
        self._airtimes_ms.clear()
        self._relay_transmissions = 0
        self._duplicates = 0
        self._failed_receptions = 0
        self._latest_failed_receptions.clear()
        self._drop_reasons.clear()
        self._drop_counts.clear()
        self._event_loop_lag_ms = None
        self._generated_count = 0
        self._submitted_count = 0
        self._submission_failed_count = 0
        self._transmitted_count = 0
        self._unique_deliveries = 0
        self._receiver_deliveries = 0
        self._receiver_opportunities = 0
        self._acknowledgments = 0
        self._rf_transmission_count = 0
        self._observed_airtime_ms = 0
        self._per_node_transmit_counts.clear()
        self._metrics = None

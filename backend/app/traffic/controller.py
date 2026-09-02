"""Fixed-rate traffic through the same ToRadio path as an external client."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.gateway import NodeGateway
from backend.app.metrics import EventBroker, EventType, MetricsSnapshot, PacketEvent, calculate_metrics
from backend.app.models import Scenario

TRAFFIC_PREFIX = "ML1"
FIRMWARE_COMMIT = "54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb"
FIRMWARE_IMAGE_DIGEST = "sha256:23e92b1331a3a471eaef0c63cbca4365ca40b3111a9781cfdbe5a5114e5773d4"
MESHTASTICATOR_COMMIT = "17ceb8231079d87b070abc6132181e4c6b20202d"
CLIENT_LIBRARY_VERSION = "2.7.11"
LOGGER = logging.getLogger(__name__)


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


class TrafficRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(alias="runId")
    state: TrafficRunState
    request: TrafficRunRequest
    scenario_snapshot: dict[str, object] = Field(alias="scenarioSnapshot")
    firmware_commit: str = Field(default=FIRMWARE_COMMIT, alias="firmwareCommit")
    firmware_image_digest: str = Field(default=FIRMWARE_IMAGE_DIGEST, alias="firmwareImageDigest")
    meshtasticator_commit: str = Field(default=MESHTASTICATOR_COMMIT, alias="meshtasticatorCommit")
    client_library_version: str = Field(default=CLIENT_LIBRARY_VERSION, alias="clientLibraryVersion")
    collision_model: Literal["native"] = Field(default="native", alias="collisionModel")
    started_at: datetime = Field(alias="startedAt")
    finished_at: datetime | None = Field(default=None, alias="finishedAt")
    random_seed: int = Field(alias="randomSeed")
    requested: int = 0
    submitted: int = 0
    submission_failed: int = Field(default=0, alias="submissionFailed")
    transmitted: int = 0
    delivered: int = 0
    generated_messages: list[GeneratedMessage] = Field(default_factory=list, alias="generatedMessages")
    metrics: MetricsSnapshot
    failure: str | None = None


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
        settle_seconds: float = 3.0,
    ) -> None:
        self.scenario = scenario
        self.gateways = gateways
        self.hardware_ids = hardware_ids
        self.event_broker = event_broker
        self.results_root = results_root
        self.settle_seconds = settle_seconds
        self.state = TrafficRunState.IDLE
        self.current: TrafficRunResult | None = None
        self._task: asyncio.Task[None] | None = None
        self._sequence = 0
        self._messages_by_packet: dict[int, GeneratedMessage] = {}
        self._messages_by_key: dict[tuple[str, int], GeneratedMessage] = {}
        self._latencies_ms: list[float] = []
        self._rf_transmitters: list[str] = []
        self._airtimes_ms: list[int] = []
        self._relay_transmissions = 0
        self._duplicates = 0
        self._failed_receptions = 0
        self._latest_failed_receptions: dict[str, int] = {}
        self._drop_reasons: list[str] = []
        self._event_loop_lag_ms: float | None = None

    def start(self, request: TrafficRunRequest) -> str:
        if self._task is not None and not self._task.done():
            raise RuntimeError("a traffic run is already active")
        self._validate_request_nodes(request)
        run_id = str(uuid.uuid4())
        marker_size = len(f"{TRAFFIC_PREFIX}:{run_id}:1:".encode())
        if request.payload_bytes < marker_size:
            raise ValueError(
                f"payloadBytes {request.payload_bytes} is smaller than encoded traffic identifier "
                f"{marker_size}"
            )
        self._reset_accumulators()
        self.current = TrafficRunResult(
            runId=run_id,
            state=TrafficRunState.RUNNING,
            request=request,
            scenarioSnapshot=self.scenario.model_dump(by_alias=True),
            startedAt=datetime.now(UTC),
            randomSeed=request.seed,
            metrics=self._snapshot_metrics(),
        )
        self.state = TrafficRunState.RUNNING
        self._task = asyncio.create_task(self._run(), name=f"traffic-{run_id}")
        LOGGER.info("traffic run started", extra={"traffic_run_id": run_id})
        return run_id

    async def stop(self) -> None:
        if self._task is None or self._task.done():
            return
        self.state = TrafficRunState.STOPPING
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)

    async def wait(self, *, deadline_seconds: float | None = None) -> TrafficRunResult:
        if self._task is not None:
            if deadline_seconds is None:
                await self._task
            else:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=deadline_seconds)
        if self.current is None:
            raise RuntimeError("no traffic run exists")
        return self.current

    def snapshot(self) -> TrafficRunResult | None:
        if self.current is None:
            return None
        delivered_keys = self._delivered_keys()
        self.current.transmitted = sum(message.transmitted for message in self.current.generated_messages)
        self.current.delivered = len(delivered_keys)
        self.current.metrics = self._snapshot_metrics(delivered_keys=delivered_keys)
        return self.current.model_copy(deep=True)

    def record_rf_transmission(
        self, transmitter: str, packet: mesh_pb2.MeshPacket, packet_airtime_ms: int
    ) -> None:
        if self.current is None or self.state not in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}:
            return
        message = self._messages_by_packet.get(packet.id)
        if message is None:
            return
        message.transmitted = True
        self._rf_transmitters.append(transmitter)
        self._airtimes_ms.append(packet_airtime_ms)
        if getattr(packet, "from") != self.hardware_ids[transmitter]:
            self._relay_transmissions += 1

    def record_drop(self, reason: str) -> None:
        if self.current is not None:
            self._drop_reasons.append(reason)

    def record_failed_receptions(self, node_id: str, total: int) -> None:
        previous = self._latest_failed_receptions.get(node_id)
        self._latest_failed_receptions[node_id] = total
        if (
            previous is not None
            and self.current is not None
            and self.state in {TrafficRunState.RUNNING, TrafficRunState.STOPPING}
        ):
            self._failed_receptions += max(0, total - previous)

    def set_event_loop_lag(self, lag_ms: float) -> None:
        self._event_loop_lag_ms = lag_ms

    async def handle_from_radio(self, node_id: str, message: mesh_pb2.FromRadio) -> None:
        if self.current is None or message.WhichOneof("payload_variant") != "packet":
            return
        packet = message.packet
        if packet.WhichOneof("payload_variant") != "decoded":
            return
        if packet.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
            parsed = self._parse_identifier(bytes(packet.decoded.payload))
            if parsed is None or parsed[0] != self.current.run_id:
                return
            _, sequence = parsed
            generated = self._messages_by_key.get((self.current.run_id, sequence))
            if generated is None:
                return
            if node_id in generated.delivered_to:
                self._duplicates += 1
                return
            generated.delivered_to.append(node_id)
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
            generated = self._messages_by_packet.get(packet.decoded.request_id)
            if generated is None:
                return
            routing = mesh_pb2.Routing()
            routing.ParseFromString(packet.decoded.payload)
            if routing.error_reason == mesh_pb2.Routing.Error.NONE:
                generated.acknowledged = True
                event_type = EventType.ACKNOWLEDGMENT
                result = "acknowledged"
            else:
                event_type = EventType.ROUTING_ERROR
                result = mesh_pb2.Routing.Error.Name(routing.error_reason)
                self._drop_reasons.append(result)
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
        randomizer = random.Random(request.seed)
        interval = 60 / request.messages_per_minute
        started = time.monotonic()
        next_send = {source: started for source in request.source_nodes}
        round_robin = {source: 0 for source in request.source_nodes}
        try:
            while True:
                source = min(next_send, key=next_send.__getitem__)
                scheduled = next_send[source]
                if scheduled - started >= request.duration_seconds:
                    break
                await asyncio.sleep(max(0, scheduled - time.monotonic()))
                destination = self._destination_for(
                    request, source, randomizer=randomizer, round_robin=round_robin
                )
                await self._submit(source, destination, randomizer)
                next_send[source] += interval
            await asyncio.sleep(self.settle_seconds)
            self.state = TrafficRunState.COMPLETED
            await self._finish(TrafficRunState.COMPLETED)
        except asyncio.CancelledError:
            self.state = TrafficRunState.CANCELLED
            await self._finish(TrafficRunState.CANCELLED)
            raise
        except Exception as exc:
            self.state = TrafficRunState.FAILED
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
        packet_id = randomizer.randrange(1, 0xFFFFFFFF)
        packet = mesh_pb2.MeshPacket(
            id=packet_id,
            to=0xFFFFFFFF if destination == "broadcast" else self.hardware_ids[destination],
            want_ack=self.current.request.acknowledgment_requested,
            hop_limit=self.scenario.rf.hop_limit,
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
        self.current.requested += 1
        self._messages_by_packet[packet_id] = generated
        self._messages_by_key[(self.current.run_id, self._sequence)] = generated
        request = mesh_pb2.ToRadio()
        request.packet.CopyFrom(packet)
        try:
            await self.gateways[source].send_to_radio(request, source="traffic")
            generated.submitted = True
            self.current.submitted += 1
        except Exception as exc:
            generated.submission_error = str(exc)
            self.current.submission_failed += 1
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
        candidates = [node.id for node in self.scenario.nodes if node.id != source]
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
        if self.current is None:
            return
        self.current.state = state
        self.current.finished_at = datetime.now(UTC)
        self.current.transmitted = sum(message.transmitted for message in self.current.generated_messages)
        delivered_keys = self._delivered_keys()
        self.current.delivered = len(delivered_keys)
        self.current.metrics = self._snapshot_metrics(delivered_keys=delivered_keys)
        self.results_root.mkdir(parents=True, exist_ok=True)
        destination = self.results_root / f"{self.current.run_id}.json"
        temporary = destination.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self.current.model_dump(mode="json", by_alias=True), indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(destination)

    def _delivered_keys(self) -> set[tuple[str, int]]:
        if self.current is None:
            return set()
        if self.current.request.kind == TrafficKind.DIRECT_TEXT:
            return {
                (self.current.run_id, message.sequence)
                for message in self.current.generated_messages
                if message.destination_node in message.delivered_to
            }
        return {
            (f"{self.current.run_id}:{receiver}", message.sequence)
            for message in self.current.generated_messages
            for receiver in message.delivered_to
            if receiver != message.source_node
        }

    def _snapshot_metrics(
        self, *, delivered_keys: set[tuple[str, int]] | None = None
    ) -> MetricsSnapshot:
        generated = len(self.current.generated_messages) if self.current is not None else 0
        receiver_keys = delivered_keys or set()
        delivered_message_ids = (
            {
                (self.current.run_id, message.sequence)
                for message in self.current.generated_messages
                if (
                    message.destination_node in message.delivered_to
                    if self.current.request.kind == TrafficKind.DIRECT_TEXT
                    else any(
                        receiver != message.source_node for receiver in message.delivered_to
                    )
                )
            }
            if self.current is not None
            else set()
        )
        receivers_per_broadcast = (
            {
                str(message.sequence): len(
                    {receiver for receiver in message.delivered_to if receiver != message.source_node}
                )
                for message in self.current.generated_messages
            }
            if self.current is not None and self.current.request.kind == TrafficKind.BROADCAST_TEXT
            else {}
        )
        receiver_opportunities = (
            generated * (len(self.scenario.nodes) - 1)
            if self.current is not None and self.current.request.kind == TrafficKind.BROADCAST_TEXT
            else generated
        )
        acknowledgments = (
            sum(message.acknowledged for message in self.current.generated_messages)
            if self.current is not None
            else 0
        )
        expected_acknowledgments = (
            generated
            if self.current is not None and self.current.request.acknowledgment_requested
            else 0
        )
        return calculate_metrics(
            generated=generated,
            delivered_ids=delivered_message_ids,
            acknowledged=acknowledgments,
            acknowledgment_expected=expected_acknowledgments,
            latencies_ms=self._latencies_ms,
            rf_transmitters=self._rf_transmitters,
            relay_transmissions=self._relay_transmissions,
            duplicate_receptions=self._duplicates,
            failed_receptions=self._failed_receptions,
            drop_reasons=self._drop_reasons,
            airtimes_ms=self._airtimes_ms,
            event_loop_lag_ms=self._event_loop_lag_ms,
            receiver_deliveries=len(receiver_keys),
            receiver_delivery_opportunities=receiver_opportunities,
            receivers_per_broadcast=receivers_per_broadcast,
        )

    def _validate_request_nodes(self, request: TrafficRunRequest) -> None:
        known = {node.id for node in self.scenario.nodes}
        unknown_sources = set(request.source_nodes) - known
        if unknown_sources:
            raise ValueError(f"unknown traffic sources: {sorted(unknown_sources)}")
        if len(set(request.source_nodes)) != len(request.source_nodes):
            raise ValueError("traffic source nodes must be unique")
        if request.fixed_destination is not None:
            if request.fixed_destination not in known:
                raise ValueError(f"unknown fixed destination: {request.fixed_destination}")
            if request.fixed_destination in request.source_nodes and len(request.source_nodes) == 1:
                raise ValueError("direct traffic destination cannot equal its only source")

    def _parse_identifier(self, payload: bytes) -> tuple[str, int] | None:
        try:
            prefix, run_id, sequence, _ = payload.decode("utf-8", errors="strict").split(":", 3)
            if prefix != TRAFFIC_PREFIX:
                return None
            return run_id, int(sequence)
        except (ValueError, UnicodeDecodeError):
            return None

    def _reset_accumulators(self) -> None:
        self._sequence = 0
        self._messages_by_packet.clear()
        self._messages_by_key.clear()
        self._latencies_ms.clear()
        self._rf_transmitters.clear()
        self._airtimes_ms.clear()
        self._relay_transmissions = 0
        self._duplicates = 0
        self._failed_receptions = 0
        self._drop_reasons.clear()

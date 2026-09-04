"""Bounded authoritative event history and bounded UI subscriptions."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    RF_TRANSMIT = "rf_transmit"
    LINK_DISABLED = "link_disabled"
    RX_INJECTED = "rx_injected"
    APPLICATION_RECEIVE = "application_receive"
    ACKNOWLEDGMENT = "acknowledgment"
    ROUTING_ERROR = "routing_error"
    LINK_UPDATED = "link_updated"
    COLLISION = "collision"
    NODE_STATE = "node_state"
    LIFECYCLE = "lifecycle"
    TRAFFIC = "traffic"
    METRICS = "metrics"
    UI_EVENTS_DROPPED = "ui_events_dropped"


class PacketEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    stream_id: str = Field(default="", alias="streamId")
    sequence: int = 0
    utc_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC), alias="utcTimestamp")
    monotonic_seconds: float = Field(alias="monotonicSeconds")
    event_type: EventType = Field(alias="eventType")
    transmitter: str | None = None
    intended_destination: str | None = Field(default=None, alias="intendedDestination")
    receiver: str | None = None
    receiver_set: list[str] = Field(default_factory=list, alias="receiverSet")
    mesh_packet_id: int | None = Field(default=None, alias="meshPacketId")
    traffic_run_id: str | None = Field(default=None, alias="trafficRunId")
    traffic_sequence: int | None = Field(default=None, alias="trafficSequence")
    hop_limit: int | None = Field(default=None, alias="hopLimit")
    hop_start: int | None = Field(default=None, alias="hopStart")
    rssi_dbm: int | None = Field(default=None, alias="rssiDbm")
    snr_db: float | None = Field(default=None, alias="snrDb")
    port_number: int | None = Field(default=None, alias="portNumber")
    packet_length: int | None = Field(default=None, alias="packetLength")
    airtime_ms: int | None = Field(default=None, alias="airtimeMs")
    metric_update: dict[str, int | float | dict[str, int] | None] = Field(
        default_factory=dict, alias="metricUpdate"
    )
    result: str | None = None
    detail: str | None = None


class EventHistoryPage(BaseModel):
    """A versioned replay page with enough state to detect an expired cursor."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    stream_id: str = Field(alias="streamId")
    stream_changed: bool = Field(alias="streamChanged")
    events: list[PacketEvent]
    first_available_sequence: int | None = Field(alias="firstAvailableSequence")
    latest_sequence: int = Field(alias="latestSequence")
    history_gap: bool = Field(alias="historyGap")
    has_more: bool = Field(alias="hasMore")


class EventSubscription:
    def __init__(self, *, buffer_size: int, stream_id: str) -> None:
        self.queue: asyncio.Queue[PacketEvent] = asyncio.Queue(maxsize=buffer_size)
        self.stream_id = stream_id
        self.dropped = 0
        self._delivered_drop_notice_last = False

    def publish(self, event: PacketEvent) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            self.queue.get_nowait()
            self.queue.put_nowait(event)

    def restart(self, stream_id: str) -> None:
        """Drop queued evidence and tell the client to restart its cursor."""

        while not self.queue.empty():
            self.queue.get_nowait()
        self.stream_id = stream_id
        self.dropped = 0
        self._delivered_drop_notice_last = False
        self.queue.put_nowait(
            PacketEvent(
                streamId=stream_id,
                monotonicSeconds=time.monotonic(),
                eventType=EventType.UI_EVENTS_DROPPED,
                result="stream-reset",
                detail="packet evidence was cleared by simulation reset",
            )
        )

    async def next(self) -> PacketEvent:
        if self.dropped and not self._delivered_drop_notice_last:
            dropped = self.dropped
            self.dropped = 0
            self._delivered_drop_notice_last = True
            return PacketEvent(
                streamId=self.stream_id,
                monotonicSeconds=asyncio.get_running_loop().time(),
                eventType=EventType.UI_EVENTS_DROPPED,
                result="dropped",
                detail=f"{dropped} UI events dropped because the client was slow",
            )
        event = await self.queue.get()
        self._delivered_drop_notice_last = False
        return event


class EventBroker:
    """Keep recent history and fan out without letting a UI stall producers."""

    def __init__(
        self,
        *,
        history_size: int = 5000,
        subscriber_buffer_size: int = 256,
        stream_id: str | None = None,
    ) -> None:
        self._history: deque[PacketEvent] = deque(maxlen=history_size)
        self._subscribers: set[EventSubscription] = set()
        self.stream_id = stream_id or str(uuid.uuid4())
        self._sequence = 0
        self.history_evictions = 0
        self.subscriber_buffer_size = subscriber_buffer_size

    def publish(self, event: PacketEvent) -> PacketEvent:
        self._sequence += 1
        sequenced = event.model_copy(
            update={"sequence": self._sequence, "stream_id": self.stream_id}
        )
        if len(self._history) == self._history.maxlen:
            self.history_evictions += 1
        self._history.append(sequenced)
        for subscriber in self._subscribers:
            subscriber.publish(sequenced)
        return sequenced

    def clear(self) -> None:
        """Clear retained evidence and restart every connected client cursor."""

        self._history.clear()
        self._sequence = 0
        self.history_evictions = 0
        self.stream_id = str(uuid.uuid4())
        for subscriber in self._subscribers:
            subscriber.restart(self.stream_id)

    def recent(
        self,
        *,
        limit: Annotated[int, Field(ge=1, le=5000)] = 250,
        node_id: str | None = None,
        event_type: EventType | None = None,
    ) -> list[PacketEvent]:
        result: list[PacketEvent] = []
        for event in reversed(self._history):
            nodes = {event.transmitter, event.receiver, *event.receiver_set}
            if node_id is not None and node_id not in nodes:
                continue
            if event_type is not None and event.event_type != event_type:
                continue
            result.append(event)
            if len(result) == limit:
                break
        result.reverse()
        return result

    def history_page(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 5000,
        stream_id: str | None = None,
    ) -> EventHistoryPage:
        """Return the earliest retained events after a client cursor."""

        retained = list(self._history)
        first_available = retained[0].sequence if retained else None
        stream_changed = stream_id is not None and stream_id != self.stream_id
        effective_after_sequence = 0 if stream_changed else after_sequence
        history_gap = (
            not stream_changed
            and after_sequence > 0
            and first_available is not None
            and after_sequence < first_available - 1
        )
        events = [event for event in retained if event.sequence > effective_after_sequence][:limit]
        return EventHistoryPage(
            streamId=self.stream_id,
            streamChanged=stream_changed,
            events=events,
            firstAvailableSequence=first_available,
            latestSequence=self._sequence,
            historyGap=history_gap,
            hasMore=bool(events) and events[-1].sequence < self._sequence,
        )

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[EventSubscription]:
        subscription = EventSubscription(
            buffer_size=self.subscriber_buffer_size,
            stream_id=self.stream_id,
        )
        self._subscribers.add(subscription)
        try:
            yield subscription
        finally:
            self._subscribers.discard(subscription)

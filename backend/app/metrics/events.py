"""Bounded authoritative event history and bounded UI subscriptions."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated

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
    UI_EVENTS_DROPPED = "ui_events_dropped"


class PacketEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    result: str | None = None
    detail: str | None = None


class EventSubscription:
    def __init__(self, *, buffer_size: int) -> None:
        self.queue: asyncio.Queue[PacketEvent] = asyncio.Queue(maxsize=buffer_size)
        self.dropped = 0

    def publish(self, event: PacketEvent) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            self.queue.get_nowait()
            self.queue.put_nowait(event)

    async def next(self) -> PacketEvent:
        if self.dropped:
            dropped = self.dropped
            self.dropped = 0
            return PacketEvent(
                monotonicSeconds=asyncio.get_running_loop().time(),
                eventType=EventType.UI_EVENTS_DROPPED,
                result="dropped",
                detail=f"{dropped} UI events dropped because the client was slow",
            )
        return await self.queue.get()


class EventBroker:
    """Keep recent history and fan out without letting a UI stall producers."""

    def __init__(self, *, history_size: int = 5000, subscriber_buffer_size: int = 256) -> None:
        self._history: deque[PacketEvent] = deque(maxlen=history_size)
        self._subscribers: set[EventSubscription] = set()
        self._sequence = 0
        self.history_evictions = 0
        self.subscriber_buffer_size = subscriber_buffer_size

    def publish(self, event: PacketEvent) -> PacketEvent:
        self._sequence += 1
        sequenced = event.model_copy(update={"sequence": self._sequence})
        if len(self._history) == self._history.maxlen:
            self.history_evictions += 1
        self._history.append(sequenced)
        for subscriber in self._subscribers:
            subscriber.publish(sequenced)
        return sequenced

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

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[EventSubscription]:
        subscription = EventSubscription(buffer_size=self.subscriber_buffer_size)
        self._subscribers.add(subscription)
        try:
            yield subscription
        finally:
            self._subscribers.discard(subscription)

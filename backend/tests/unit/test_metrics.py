from __future__ import annotations

import asyncio

import pytest
from meshtastic.protobuf import mesh_pb2, portnums_pb2

from backend.app.metrics import (
    EventBroker,
    EventType,
    PacketEvent,
    airtime_ms,
    calculate_metrics,
    maximum_retransmission_delay_ms,
    mesh_packet_payload_length,
    mesh_packet_port_number,
    percentile,
)


@pytest.mark.parametrize(
    ("preset", "expected"),
    [
        ("SHORT_FAST", 89),
        ("MEDIUM_FAST", 289),
        ("LONG_FAST", 976),
        ("LONG_MODERATE", 3379),
        ("LONG_SLOW", 6168),
    ],
)
def test_airtime_matches_upstream_simradio_equation(preset: str, expected: int) -> None:
    assert airtime_ms(100, preset) == expected


def test_retransmission_delay_includes_airtime_contention_and_processing() -> None:
    assert maximum_retransmission_delay_ms(100, "LONG_SLOW") == 44051


def test_airtime_rounds_payload_symbols_after_applying_coding_rate() -> None:
    assert airtime_ms(96, "LONG_SLOW") == 5939
    assert maximum_retransmission_delay_ms(96, "LONG_SLOW") == 43593


def test_simulator_wrapper_is_not_counted_as_extra_airtime() -> None:
    compressed = mesh_pb2.Compressed(portnum=portnums_pb2.TEXT_MESSAGE_APP, data=b"hello")
    wrapped = mesh_pb2.MeshPacket()
    wrapped.decoded.portnum = portnums_pb2.SIMULATOR_APP
    wrapped.decoded.payload = compressed.SerializeToString()

    original = mesh_pb2.Data(portnum=portnums_pb2.TEXT_MESSAGE_APP, payload=b"hello")

    assert mesh_packet_payload_length(wrapped) == len(original.SerializeToString()) + 16
    assert mesh_packet_port_number(wrapped) == portnums_pb2.TEXT_MESSAGE_APP


def test_percentiles_are_unavailable_with_too_few_samples() -> None:
    assert percentile([10], 0.5) == 10
    assert percentile([10] * 19, 0.95, minimum_samples=20) is None
    assert percentile(list(range(20)), 0.95, minimum_samples=20) == pytest.approx(18.05)


def test_metrics_count_airtime_once_per_transmitter() -> None:
    metrics = calculate_metrics(
        generated=2,
        delivered_ids={("run", 1)},
        acknowledged=1,
        acknowledgment_expected=2,
        latencies_ms=[100],
        rf_transmitters=["node-1", "node-2", "node-2"],
        relay_transmissions=2,
        duplicate_receptions=1,
        failed_receptions=1,
        drop_reasons=["link-disabled", "link-disabled"],
        airtimes_ms=[10, 20, 20],
        receiver_deliveries=3,
        receiver_delivery_opportunities=4,
        receivers_per_broadcast={"1": 2, "2": 1},
    )

    assert metrics.delivery_ratio == 0.5
    assert metrics.receiver_deliveries == 3
    assert metrics.receiver_delivery_ratio == 0.75
    assert metrics.receivers_per_broadcast == {"1": 2, "2": 1}
    assert metrics.rf_transmissions == 3
    assert metrics.rf_transmissions_per_delivery == 3
    assert metrics.observed_airtime_ms == 50
    assert metrics.per_node_transmit_counts == {"node-1": 1, "node-2": 2}
    assert metrics.per_node_airtime_ms == {"node-1": 10, "node-2": 40}
    assert metrics.drops_by_reason == {"link-disabled": 2}


@pytest.mark.asyncio
async def test_bounded_event_subscription_reports_drops() -> None:
    broker = EventBroker(history_size=2, subscriber_buffer_size=1)
    async with broker.subscribe() as subscription:
        for index in range(3):
            broker.publish(
                PacketEvent(
                    monotonicSeconds=float(index),
                    eventType=EventType.RF_TRANSMIT,
                    meshPacketId=index,
                )
            )
        dropped = await asyncio.wait_for(subscription.next(), timeout=1)
        latest = await asyncio.wait_for(subscription.next(), timeout=1)

    assert dropped.event_type == EventType.UI_EVENTS_DROPPED
    assert dropped.detail == "2 UI events dropped because the client was slow"
    assert latest.mesh_packet_id == 2
    assert broker.history_evictions == 1
    assert [event.mesh_packet_id for event in broker.recent()] == [1, 2]


@pytest.mark.asyncio
async def test_drop_notices_do_not_starve_buffered_events() -> None:
    broker = EventBroker(subscriber_buffer_size=1)
    async with broker.subscribe() as subscription:
        for packet_id in range(3):
            broker.publish(
                PacketEvent(
                    monotonicSeconds=float(packet_id),
                    eventType=EventType.RF_TRANSMIT,
                    meshPacketId=packet_id,
                )
            )
        first_notice = await asyncio.wait_for(subscription.next(), timeout=1)
        broker.publish(
            PacketEvent(
                monotonicSeconds=3,
                eventType=EventType.RF_TRANSMIT,
                meshPacketId=3,
            )
        )
        buffered_event = await asyncio.wait_for(subscription.next(), timeout=1)
        second_notice = await asyncio.wait_for(subscription.next(), timeout=1)

    assert first_notice.event_type == EventType.UI_EVENTS_DROPPED
    assert buffered_event.mesh_packet_id == 3
    assert second_notice.event_type == EventType.UI_EVENTS_DROPPED
    assert second_notice.detail == "1 UI events dropped because the client was slow"

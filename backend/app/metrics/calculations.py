"""Pure metric calculations used by live and persisted traffic results."""

from __future__ import annotations

import math
from collections import Counter

from pydantic import BaseModel, ConfigDict, Field


class _MetricsBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_application_messages: int = Field(alias="generatedApplicationMessages")
    unique_application_messages_delivered: int = Field(alias="uniqueApplicationMessagesDelivered")
    delivery_ratio: float | None = Field(alias="deliveryRatio")
    receiver_deliveries: int = Field(alias="receiverDeliveries")
    receiver_delivery_ratio: float | None = Field(alias="receiverDeliveryRatio")
    acknowledgment_success_ratio: float | None = Field(alias="acknowledgmentSuccessRatio")
    median_latency_ms: float | None = Field(alias="medianLatencyMs")
    p95_latency_ms: float | None = Field(alias="p95LatencyMs")
    p99_latency_ms: float | None = Field(alias="p99LatencyMs")
    rf_transmissions: int = Field(alias="rfTransmissions")
    rf_transmissions_per_delivery: float | None = Field(alias="rfTransmissionsPerDelivery")
    relay_transmissions: int = Field(alias="relayTransmissions")
    duplicate_receptions: int = Field(alias="duplicateReceptions")
    failed_receptions: int = Field(alias="failedReceptions")
    drops_by_reason: dict[str, int] = Field(alias="dropsByReason")
    observed_airtime_ms: int = Field(alias="observedAirtimeMs")
    per_node_transmit_counts: dict[str, int] = Field(alias="perNodeTransmitCounts")
    per_node_airtime_ms: dict[str, int] = Field(default_factory=dict, alias="perNodeAirtimeMs")
    event_loop_lag_ms: float | None = Field(default=None, alias="eventLoopLagMs")


class MetricsSnapshot(_MetricsBase):
    """Complete metrics, including the per-message broadcast detail."""

    receivers_per_broadcast: dict[str, int] = Field(alias="receiversPerBroadcast")


class MetricsSummary(_MetricsBase):
    """Bounded live metrics without the per-message broadcast detail."""


def percentile(values: list[float], proportion: float, *, minimum_samples: int = 1) -> float | None:
    if len(values) < minimum_samples:
        return None
    if not 0 <= proportion <= 1:
        raise ValueError("percentile proportion must be between zero and one")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * proportion
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _percentile_ordered(
    ordered: list[float], proportion: float, *, minimum_samples: int = 1
) -> float | None:
    if len(ordered) < minimum_samples:
        return None
    if not 0 <= proportion <= 1:
        raise ValueError("percentile proportion must be between zero and one")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * proportion
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def calculate_metrics(
    *,
    generated: int,
    delivered_ids: set[tuple[str, int]],
    delivered_count: int | None = None,
    acknowledged: int,
    acknowledgment_expected: int,
    latencies_ms: list[float],
    rf_transmitters: list[str],
    relay_transmissions: int,
    duplicate_receptions: int,
    failed_receptions: int,
    drop_reasons: list[str],
    airtimes_ms: list[int],
    event_loop_lag_ms: float | None = None,
    receiver_deliveries: int | None = None,
    receiver_delivery_opportunities: int | None = None,
    receivers_per_broadcast: dict[str, int] | None = None,
    rf_transmission_count: int | None = None,
    observed_airtime_ms: int | None = None,
    per_node_transmit_counts: dict[str, int] | None = None,
    per_node_airtime_ms: dict[str, int] | None = None,
    drops_by_reason: dict[str, int] | None = None,
) -> MetricsSnapshot:
    delivered = len(delivered_ids) if delivered_count is None else delivered_count
    receiver_count = delivered if receiver_deliveries is None else receiver_deliveries
    receiver_opportunities = (
        generated
        if receiver_delivery_opportunities is None
        else receiver_delivery_opportunities
    )
    rf_count = len(rf_transmitters) if rf_transmission_count is None else rf_transmission_count
    if per_node_airtime_ms is None:
        airtime_by_transmitter: Counter[str] = Counter()
        for transmitter, packet_airtime_ms in zip(rf_transmitters, airtimes_ms, strict=True):
            airtime_by_transmitter[transmitter] += packet_airtime_ms
        per_node_airtime_ms = dict(airtime_by_transmitter)
    # Sort once and reuse the ordered sample for all three exact percentiles.
    ordered_latencies = sorted(latencies_ms)
    return MetricsSnapshot(
        generatedApplicationMessages=generated,
        uniqueApplicationMessagesDelivered=delivered,
        deliveryRatio=delivered / generated if generated else None,
        receiverDeliveries=receiver_count,
        receiverDeliveryRatio=(
            receiver_count / receiver_opportunities if receiver_opportunities else None
        ),
        receiversPerBroadcast=receivers_per_broadcast or {},
        acknowledgmentSuccessRatio=(
            acknowledged / acknowledgment_expected if acknowledgment_expected else None
        ),
        medianLatencyMs=_percentile_ordered(ordered_latencies, 0.5),
        p95LatencyMs=_percentile_ordered(ordered_latencies, 0.95, minimum_samples=20),
        p99LatencyMs=_percentile_ordered(ordered_latencies, 0.99, minimum_samples=100),
        rfTransmissions=rf_count,
        rfTransmissionsPerDelivery=rf_count / delivered if delivered else None,
        relayTransmissions=relay_transmissions,
        duplicateReceptions=duplicate_receptions,
        failedReceptions=failed_receptions,
        dropsByReason=(dict(Counter(drop_reasons)) if drops_by_reason is None else drops_by_reason),
        observedAirtimeMs=(sum(airtimes_ms) if observed_airtime_ms is None else observed_airtime_ms),
        perNodeTransmitCounts=(
            dict(Counter(rf_transmitters))
            if per_node_transmit_counts is None
            else per_node_transmit_counts
        ),
        perNodeAirtimeMs=per_node_airtime_ms,
        eventLoopLagMs=event_loop_lag_ms,
    )

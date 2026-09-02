"""Pure metric calculations used by live and persisted traffic results."""

from __future__ import annotations

import math
from collections import Counter

from pydantic import BaseModel, ConfigDict, Field


class MetricsSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_application_messages: int = Field(alias="generatedApplicationMessages")
    unique_application_messages_delivered: int = Field(alias="uniqueApplicationMessagesDelivered")
    delivery_ratio: float | None = Field(alias="deliveryRatio")
    receiver_deliveries: int = Field(alias="receiverDeliveries")
    receiver_delivery_ratio: float | None = Field(alias="receiverDeliveryRatio")
    receivers_per_broadcast: dict[str, int] = Field(alias="receiversPerBroadcast")
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
    event_loop_lag_ms: float | None = Field(default=None, alias="eventLoopLagMs")


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


def calculate_metrics(
    *,
    generated: int,
    delivered_ids: set[tuple[str, int]],
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
) -> MetricsSnapshot:
    delivered = len(delivered_ids)
    receiver_count = delivered if receiver_deliveries is None else receiver_deliveries
    receiver_opportunities = (
        generated
        if receiver_delivery_opportunities is None
        else receiver_delivery_opportunities
    )
    rf_count = len(rf_transmitters)
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
        medianLatencyMs=percentile(latencies_ms, 0.5),
        p95LatencyMs=percentile(latencies_ms, 0.95, minimum_samples=20),
        p99LatencyMs=percentile(latencies_ms, 0.99, minimum_samples=100),
        rfTransmissions=rf_count,
        rfTransmissionsPerDelivery=rf_count / receiver_count if receiver_count else None,
        relayTransmissions=relay_transmissions,
        duplicateReceptions=duplicate_receptions,
        failedReceptions=failed_receptions,
        dropsByReason=dict(Counter(drop_reasons)),
        observedAirtimeMs=sum(airtimes_ms),
        perNodeTransmitCounts=dict(Counter(rf_transmitters)),
        eventLoopLagMs=event_loop_lag_ms,
    )

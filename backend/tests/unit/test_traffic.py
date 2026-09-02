from __future__ import annotations

from pathlib import Path

import pytest
from meshtastic.protobuf import mesh_pb2

from backend.app.metrics import EventBroker
from backend.app.models import default_scenario
from backend.app.traffic import (
    DestinationStrategy,
    TrafficController,
    TrafficKind,
    TrafficRunRequest,
    TrafficRunState,
)


class FakeGateway:
    def __init__(self) -> None:
        self.sent: list[mesh_pb2.ToRadio] = []

    async def send_to_radio(self, message: mesh_pb2.ToRadio, *, source: str = "controller") -> None:
        del source
        copy = mesh_pb2.ToRadio()
        copy.CopyFrom(message)
        self.sent.append(copy)


@pytest.mark.asyncio
async def test_deterministic_traffic_schedule_and_persistence(tmp_path: Path) -> None:
    scenario = default_scenario(3)
    gateways = {node.id: FakeGateway() for node in scenario.nodes}
    controller = TrafficController(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2, "node-3": 3},
        event_broker=EventBroker(),
        results_root=tmp_path,
        settle_seconds=0,
    )
    request = TrafficRunRequest(
        kind=TrafficKind.DIRECT_TEXT,
        sourceNodes=["node-1"],
        destinationStrategy=DestinationStrategy.DETERMINISTIC_RANDOM,
        messagesPerMinute=600,
        payloadBytes=64,
        durationSeconds=0.21,
        acknowledgmentRequested=True,
        seed=7,
    )

    run_id = controller.start(request)
    result = await controller.wait(deadline_seconds=2)

    assert result.run_id == run_id
    assert result.state == TrafficRunState.COMPLETED
    assert result.requested == 3
    assert result.submitted == 3
    assert [message.packet_id for message in result.generated_messages] == [
        4071050725,
        1695753999,
        311111476,
    ]
    assert (tmp_path / f"{run_id}.json").is_file()


def test_payload_is_validated_after_identifier_encoding(tmp_path: Path) -> None:
    scenario = default_scenario(2)
    controller = TrafficController(
        scenario=scenario,
        gateways={"node-1": FakeGateway(), "node-2": FakeGateway()},  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path,
    )
    request = TrafficRunRequest(
        kind=TrafficKind.BROADCAST_TEXT,
        sourceNodes=["node-1"],
        payloadBytes=16,
        durationSeconds=1,
    )

    with pytest.raises(ValueError, match="encoded traffic identifier"):
        controller.start(request)

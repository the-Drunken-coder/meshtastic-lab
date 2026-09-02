from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from meshtastic.protobuf import mesh_pb2

import backend.app.simulator.service as service_module
from backend.app.gateway import GatewayEvent
from backend.app.metrics import EventBroker
from backend.app.models import (
    DirectedLink,
    TopologyPreset,
    apply_topology_preset,
    default_scenario,
)
from backend.app.simulator import LifecycleState, SimulationConflict, SimulatorService
from backend.app.traffic import TrafficController, TrafficRunRequest


class FakeGateway:
    def __init__(self) -> None:
        self.sent: list[mesh_pb2.ToRadio] = []

    async def send_to_radio(self, message: mesh_pb2.ToRadio, *, source: str) -> None:
        del source
        copied = mesh_pb2.ToRadio()
        copied.CopyFrom(message)
        self.sent.append(copied)


class FakeMedium:
    def __init__(self) -> None:
        self.links: list[DirectedLink] = []

    async def update_link(self, link: DirectedLink) -> None:
        self.links.append(link)


class WarmupGateway:
    control_host = "127.0.0.1"
    control_port = 1

    def __init__(self) -> None:
        self.client_disconnected = asyncio.Event()
        self.client_disconnected.set()


@pytest.mark.asyncio
async def test_runtime_link_snapshot_is_atomic_with_traffic_start(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.scenario = default_scenario(2)
    service.state = LifecycleState.RUNNING
    medium = FakeMedium()
    service.medium = medium  # type: ignore[assignment]
    gateways = {node.id: FakeGateway() for node in service.scenario.nodes}
    service.traffic = TrafficController(
        scenario=service.scenario,
        gateways=gateways,  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=tmp_path / "runs",
        settle_seconds=0,
    )

    changed = DirectedLink.model_validate(
        {"from": "node-1", "to": "node-2", "enabled": False}
    )
    await service.update_link(changed)
    await service.start_traffic(
        TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=0.1,
            durationSeconds=1,
            payloadBytes=64,
        )
    )

    assert service.traffic.current is not None
    links = service.traffic.current.scenario_snapshot["links"]
    assert isinstance(links, list)
    changed_snapshot = next(
        link for link in links if link["from"] == "node-1" and link["to"] == "node-2"
    )
    assert changed_snapshot["enabled"] is False
    with pytest.raises(SimulationConflict, match="only between traffic runs"):
        await service.update_link(
            DirectedLink.model_validate(
                {"from": "node-1", "to": "node-2", "enabled": True}
            )
        )
    await service.traffic.stop()


@pytest.mark.asyncio
async def test_ten_node_line_warmup_is_best_effort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    line = apply_topology_preset(default_scenario(10), TopologyPreset.LINE)
    service.scenario = line.model_copy(
        deep=True, update={"rf": line.rf.model_copy(update={"hop_limit": 7})}
    )
    service.gateways = {
        node.id: WarmupGateway() for node in service.scenario.nodes
    }  # type: ignore[assignment]

    def unavailable_nodeinfo(**_kwargs: object) -> int:
        raise RuntimeError("no route observation")

    monkeypatch.setattr(service_module, "request_node_info", unavailable_nodeinfo)

    missing, expected = await service._warm_up_nodes()

    assert expected == len(service.scenario.reachable_pairs())
    assert ("node-1", "node-10") in missing


@pytest.mark.asyncio
async def test_rf_queue_overflow_fails_with_explicit_category(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, warmup_seconds=0)
    service.state = LifecycleState.RUNNING

    await service._on_gateway_event(
        GatewayEvent(
            node_id="node-1",
            kind="gateway.rf_queue_full",
            detail="RF frame dropped from controller queue",
        )
    )
    assert service._failure_cleanup_task is not None
    await service._failure_cleanup_task

    assert service.state == LifecycleState.FAILED
    assert service.message.startswith("SIMULATOR_OVERLOAD:")

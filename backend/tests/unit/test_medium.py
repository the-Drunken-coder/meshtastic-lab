from __future__ import annotations

import asyncio

import pytest
from meshtastic.protobuf import mesh_pb2, portnums_pb2

from backend.app.metrics import EventBroker, EventType
from backend.app.models import DirectedLink, TopologyPreset, apply_topology_preset, default_scenario
from backend.app.simulator import DirectedMedium


class FakeGateway:
    def __init__(self) -> None:
        self.rf_frames: asyncio.Queue[mesh_pb2.MeshPacket] = asyncio.Queue()
        self.injected: list[mesh_pb2.MeshPacket] = []

    async def inject_simulated_packet(self, packet: mesh_pb2.MeshPacket) -> None:
        copy = mesh_pb2.MeshPacket()
        copy.CopyFrom(packet)
        self.injected.append(copy)


class FailingGateway(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.rf_frames = asyncio.Queue(maxsize=1)

    async def inject_simulated_packet(self, packet: mesh_pb2.MeshPacket) -> None:
        raise asyncio.QueueFull


def text_packet(packet_id: int = 7) -> mesh_pb2.MeshPacket:
    compressed = mesh_pb2.Compressed(portnum=portnums_pb2.TEXT_MESSAGE_APP, data=b"medium")
    packet = mesh_pb2.MeshPacket(id=packet_id, to=0xFFFFFFFF, hop_limit=4, hop_start=4)
    setattr(packet, "from", 0xA11CE001)
    packet.decoded.portnum = portnums_pb2.SIMULATOR_APP
    packet.decoded.payload = compressed.SerializeToString()
    return packet


@pytest.mark.asyncio
async def test_asymmetric_medium_injects_only_enabled_direction() -> None:
    scenario = apply_topology_preset(default_scenario(2), TopologyPreset.ALL_ISOLATED)
    links = [
        DirectedLink(**{"from": "node-1", "to": "node-2", "enabled": True}),
        DirectedLink(**{"from": "node-2", "to": "node-1", "enabled": False}),
    ]
    scenario = scenario.model_copy(update={"links": links})
    gateways = {"node-1": FakeGateway(), "node-2": FakeGateway()}
    broker = EventBroker()
    medium = DirectedMedium(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        event_broker=broker,
        hardware_ids={"node-1": 0xA11CE001, "node-2": 0xA11CE002},
    )

    await medium.transmit("node-1", text_packet())
    await medium.transmit("node-2", text_packet(8))

    assert len(gateways["node-2"].injected) == 1
    assert gateways["node-2"].injected[0].rx_rssi == -85
    assert gateways["node-1"].injected == []
    assert sum(event.event_type == EventType.RF_TRANSMIT for event in broker.recent()) == 2
    assert all(
        event.port_number == portnums_pb2.TEXT_MESSAGE_APP
        for event in broker.recent()
        if event.event_type in {EventType.RF_TRANSMIT, EventType.LINK_DISABLED, EventType.RX_INJECTED}
    )


@pytest.mark.asyncio
async def test_atomic_runtime_link_update_changes_subsequent_transmission() -> None:
    scenario = default_scenario(2)
    gateways = {"node-1": FakeGateway(), "node-2": FakeGateway()}
    broker = EventBroker()
    medium = DirectedMedium(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        event_broker=broker,
        hardware_ids={"node-1": 0xA11CE001, "node-2": 0xA11CE002},
    )

    await medium.update_link(
        DirectedLink(**{"from": "node-1", "to": "node-2", "enabled": False})
    )
    await medium.transmit("node-1", text_packet())

    assert gateways["node-2"].injected == []
    assert any(event.event_type == EventType.LINK_DISABLED for event in broker.recent())


@pytest.mark.asyncio
async def test_medium_worker_reports_receiver_injection_failure() -> None:
    scenario = default_scenario(2)
    gateways = {"node-1": FakeGateway(), "node-2": FailingGateway()}
    broker = EventBroker()
    failures: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()

    async def record_failure(node_id: str, exc: Exception) -> None:
        await failures.put((node_id, exc))

    medium = DirectedMedium(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        event_broker=broker,
        hardware_ids={"node-1": 0xA11CE001, "node-2": 0xA11CE002},
        failure_handler=record_failure,
    )

    await medium.start()
    await gateways["node-1"].rf_frames.put(text_packet())
    node_id, exc = await asyncio.wait_for(failures.get(), timeout=1)

    assert node_id == "node-1"
    assert isinstance(exc, asyncio.QueueFull)
    await medium.stop()


@pytest.mark.asyncio
async def test_stopping_medium_does_not_report_worker_cancellation() -> None:
    scenario = default_scenario(2)
    gateways = {"node-1": FakeGateway(), "node-2": FakeGateway()}
    broker = EventBroker()
    failures: list[tuple[str, Exception]] = []

    async def record_failure(node_id: str, exc: Exception) -> None:
        failures.append((node_id, exc))

    medium = DirectedMedium(
        scenario=scenario,
        gateways=gateways,  # type: ignore[arg-type]
        event_broker=broker,
        hardware_ids={"node-1": 0xA11CE001, "node-2": 0xA11CE002},
        failure_handler=record_failure,
    )

    await medium.start()
    await medium.stop()

    assert failures == []

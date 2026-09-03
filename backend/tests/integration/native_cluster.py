from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import uuid

from meshtastic.protobuf import mesh_pb2, portnums_pb2

from backend.app.gateway import GatewayEvent, NodeGateway
from backend.app.metrics import EventBroker
from backend.app.models import Scenario
from backend.app.runtime import (
    NodeVerification,
    configure_and_verify_node,
    request_node_info,
    verify_node,
)
from backend.app.simulator.medium import DirectedMedium

FIRMWARE_IMAGE = os.environ.get(
    "MESHTASTIC_TEST_FIRMWARE_IMAGE",
    "meshtastic/meshtasticd@"
    "sha256:23e92b1331a3a471eaef0c63cbca4365ca40b3111a9781cfdbe5a5114e5773d4",
)


def unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class NativeDockerCluster:
    """Test-only Docker wrapper around production gateways and RF medium."""

    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self.container_names: list[str] = []
        self.container_by_node: dict[str, str] = {}
        self.gateways: dict[str, NodeGateway] = {}
        self.verifications: dict[str, NodeVerification] = {}
        self.gateway_events: list[GatewayEvent] = []
        self.medium: DirectedMedium | None = None
        self.event_broker = EventBroker(history_size=10000)
        self.nodeinfo_observations: set[tuple[str, str]] = set()
        self.nodeinfo_changed = asyncio.Event()

    async def start(self) -> None:
        daemon_ports = {node.id: unused_port() for node in self.scenario.nodes}
        public_ports = {node.id: unused_port() for node in self.scenario.nodes}
        try:
            for index, node in enumerate(self.scenario.nodes):
                name = f"ml-native-{os.getpid()}-{uuid.uuid4().hex[:8]}"
                self.container_names.append(name)
                self.container_by_node[node.id] = name
                result = await asyncio.to_thread(
                    subprocess.run,
                    [
                        "docker",
                        "run",
                        "--detach",
                        "--rm",
                        "--name",
                        name,
                        "--publish",
                        f"127.0.0.1:{daemon_ports[node.id]}:46001",
                        FIRMWARE_IMAGE,
                        "/usr/bin/meshtasticd",
                        "--erase",
                        "--sim",
                        "--fsdir",
                        f"/tmp/{node.id}",
                        "--hwid",
                        str(0xA11CE001 + index),
                        "--port",
                        "46001",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                if not result.stdout.strip():
                    raise RuntimeError(f"container did not start for {node.id}")
                self.gateways[node.id] = NodeGateway(
                    node_id=node.id,
                    downstream_host="127.0.0.1",
                    downstream_port=daemon_ports[node.id],
                    public_host="127.0.0.1",
                    public_port=public_ports[node.id],
                    event_handler=self._record_gateway_event,
                    from_radio_handler=self._record_from_radio,
                    startup_timeout=30,
                )

            await asyncio.gather(*(gateway.start() for gateway in self.gateways.values()))

            async def configure(node_id: str) -> NodeVerification:
                node = next(item for item in self.scenario.nodes if item.id == node_id)
                verification = await asyncio.to_thread(
                    configure_and_verify_node,
                    hostname="127.0.0.1",
                    port=public_ports[node_id],
                    node=node,
                    rf=self.scenario.rf,
                    channel=self.scenario.channel,
                    reboot_after_apply=True,
                )
                await asyncio.wait_for(self.gateways[node_id].client_disconnected.wait(), timeout=5)
                return verification

            configured = await asyncio.gather(*(configure(node.id) for node in self.scenario.nodes))
            del configured

            async def reconnect_and_verify(node_id: str) -> NodeVerification:
                gateway = self.gateways[node_id]
                await asyncio.wait_for(gateway.failed.wait(), timeout=12)
                node = next(item for item in self.scenario.nodes if item.id == node_id)
                for _attempt in range(3):
                    await gateway.stop()
                    await gateway.start()
                    verification = await asyncio.to_thread(
                        verify_node,
                        hostname="127.0.0.1",
                        port=public_ports[node_id],
                        node=node,
                        rf=self.scenario.rf,
                        channel=self.scenario.channel,
                    )
                    await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)
                    try:
                        await asyncio.wait_for(gateway.failed.wait(), timeout=11)
                    except TimeoutError:
                        return verification
                raise RuntimeError(f"{node_id} did not stabilize after configuration restarts")

            restarted = await asyncio.gather(
                *(reconnect_and_verify(node.id) for node in self.scenario.nodes)
            )
            self.verifications = {item.node_id: item for item in restarted}
            self.medium = DirectedMedium(
                scenario=self.scenario,
                gateways=self.gateways,
                event_broker=self.event_broker,
                hardware_ids={item.node_id: item.node_number for item in restarted},
            )
            await self.medium.start()
            await self._warm_up(public_ports)
        except Exception:
            await self.stop()
            raise

    async def _record_gateway_event(self, event: GatewayEvent) -> None:
        self.gateway_events.append(event)

    async def _record_from_radio(self, node_id: str, message: mesh_pb2.FromRadio) -> None:
        if message.WhichOneof("payload_variant") != "packet":
            return
        packet = message.packet
        if (
            packet.WhichOneof("payload_variant") != "decoded"
            or packet.decoded.portnum != portnums_pb2.NODEINFO_APP
        ):
            return
        source_number = int(getattr(packet, "from"))
        source = next(
            (
                candidate
                for candidate, verification in self.verifications.items()
                if verification.node_number == source_number
            ),
            None,
        )
        if source is not None and source != node_id:
            self.nodeinfo_observations.add((source, node_id))
            self.nodeinfo_changed.set()

    async def _warm_up(self, public_ports: dict[str, int]) -> None:
        expected = self.scenario.reachable_pairs(max_hops=self.scenario.rf.hop_limit)
        if not expected:
            return
        self.nodeinfo_observations.clear()
        deadline_seconds = min(120, 30 + 10 * len(self.scenario.nodes))
        try:
            async with asyncio.timeout(deadline_seconds):
                for node in self.scenario.nodes:
                    source_expected = {pair for pair in expected if pair[0] == node.id}
                    while source_expected - self.nodeinfo_observations:
                        await asyncio.to_thread(
                            request_node_info,
                            hostname="127.0.0.1",
                            port=public_ports[node.id],
                        )
                        await self.gateways[node.id].client_disconnected.wait()
                        if source_expected.issubset(self.nodeinfo_observations):
                            break
                        self.nodeinfo_changed.clear()
                        if source_expected.issubset(self.nodeinfo_observations):
                            break
                        try:
                            await asyncio.wait_for(self.nodeinfo_changed.wait(), timeout=3)
                        except TimeoutError:
                            continue
        except TimeoutError as exc:
            missing = sorted(expected - self.nodeinfo_observations)
            raise RuntimeError(
                f"firmware warm-up did not observe reachable pairs within "
                f"{deadline_seconds}s: {missing}"
            ) from exc
        await asyncio.sleep(2)

    async def stop(self) -> None:
        try:
            if self.medium is not None:
                await self.medium.stop()
                self.medium = None
            if self.gateways:
                await asyncio.gather(*(gateway.stop() for gateway in self.gateways.values()))
                self.gateways.clear()
        finally:
            for name in self.container_names:
                await asyncio.to_thread(
                    subprocess.run,
                    ["docker", "rm", "--force", name],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            self.container_names.clear()
            self.container_by_node.clear()

    def daemon_log_tail(self) -> str:
        tails: list[str] = []
        for node_id, name in self.container_by_node.items():
            result = subprocess.run(
                ["docker", "logs", "--tail", "80", name],
                check=False,
                capture_output=True,
                text=True,
            )
            output = (result.stdout + result.stderr).strip()
            tails.append(f"[{node_id}]\n{output}")
        return "\n".join(tails)

    def public_port(self, node_id: str) -> int:
        return self.gateways[node_id].public_port

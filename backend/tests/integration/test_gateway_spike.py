from __future__ import annotations

import asyncio
import contextlib
import os
import queue
import socket
import subprocess

import pytest
from meshtastic import tcp_interface
from meshtastic.protobuf import mesh_pb2, portnums_pb2
from pubsub import pub

from backend.app.gateway import NodeGateway
from backend.app.models import RFSettings, ScenarioChannel, ScenarioNode
from backend.app.runtime import configure_and_verify_node

FIRMWARE_IMAGE = (
    "meshtastic/meshtasticd@"
    "sha256:23e92b1331a3a471eaef0c63cbca4365ca40b3111a9781cfdbe5a5114e5773d4"
)


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_official_client_gateway_handshake_rf_and_reconnect() -> None:
    daemon_port = _unused_port()
    public_port = _unused_port()
    container_name = f"meshtastic-lab-gateway-spike-{os.getpid()}"
    run = await asyncio.to_thread(
        subprocess.run,
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "--publish",
            f"127.0.0.1:{daemon_port}:46001",
            FIRMWARE_IMAGE,
            "/usr/bin/meshtasticd",
            "--erase",
            "--sim",
            "--fsdir",
            "/tmp/node-1",
            "--hwid",
            "16",
            "--port",
            "46001",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert run.stdout.strip()

    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=daemon_port,
        public_host="127.0.0.1",
        public_port=public_port,
        public_clients_enabled=False,
    )
    internal: tcp_interface.TCPInterface | None = None
    first: tcp_interface.TCPInterface | None = None
    second: tcp_interface.TCPInterface | None = None
    received: queue.Queue[dict[str, object]] = queue.Queue()

    def on_receive(packet: dict[str, object], interface: object) -> None:
        del interface
        decoded = packet.get("decoded")
        if isinstance(decoded, dict) and decoded.get("text") == "inbound-spike":
            received.put(packet)

    pub.subscribe(on_receive, "meshtastic.receive.text")
    try:
        await gateway.start()

        blocked_reader, blocked_writer = await asyncio.open_connection("127.0.0.1", public_port)
        assert await asyncio.wait_for(blocked_reader.read(1), timeout=2) == b""
        blocked_writer.close()
        await blocked_writer.wait_closed()

        internal = await asyncio.to_thread(
            tcp_interface.TCPInterface,
            hostname=gateway.control_host,
            portNumber=gateway.control_port,
            timeout=20,
        )
        assert internal.myInfo is not None
        assert not gateway.external_connected
        await asyncio.to_thread(internal.close)
        internal = None
        await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)
        await gateway.enable_public_clients()

        first = await asyncio.to_thread(
            tcp_interface.TCPInterface,
            hostname="127.0.0.1",
            portNumber=public_port,
            timeout=20,
        )
        assert first.myInfo is not None
        assert first.myInfo.my_node_num != 0
        assert first.nodesByNum

        await asyncio.to_thread(first.sendText, "gateway-spike", wantAck=True)
        outgoing = await asyncio.wait_for(gateway.rf_frames.get(), timeout=20)
        carried = mesh_pb2.Compressed()
        carried.ParseFromString(outgoing.decoded.payload)
        assert carried.portnum == portnums_pb2.TEXT_MESSAGE_APP
        assert carried.data == b"gateway-spike"

        extra_reader, extra_writer = await asyncio.open_connection("127.0.0.1", public_port)
        assert await asyncio.wait_for(extra_reader.read(1), timeout=2) == b""
        extra_writer.close()
        await extra_writer.wait_closed()
        assert gateway.rejected_clients == 2

        compressed = mesh_pb2.Compressed(portnum=portnums_pb2.TEXT_MESSAGE_APP, data=b"inbound-spike")
        injected = mesh_pb2.MeshPacket(
            to=first.myInfo.my_node_num,
            id=0xA11CE,
            hop_limit=3,
            hop_start=3,
            rx_rssi=-82,
            rx_snr=7.5,
        )
        setattr(injected, "from", 0x22222222)
        injected.decoded.portnum = portnums_pb2.SIMULATOR_APP
        injected.decoded.payload = compressed.SerializeToString()
        await gateway.inject_simulated_packet(injected)
        inbound = await asyncio.to_thread(received.get, True, 20)
        assert inbound["from"] == 0x22222222

        await asyncio.to_thread(first.close)
        first = None
        await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)

        second = await asyncio.to_thread(
            tcp_interface.TCPInterface,
            hostname="127.0.0.1",
            portNumber=public_port,
            timeout=20,
        )
        assert second.myInfo is not None
        assert second.myInfo.my_node_num != 0
        assert second.nodesByNum

        await asyncio.to_thread(second.close)
        second = None
        await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=5)
        verification = await asyncio.to_thread(
            configure_and_verify_node,
            hostname="127.0.0.1",
            port=public_port,
            node=ScenarioNode(id="node-1", displayName="Node 1", role="CLIENT", apiPort=45001),
            rf=RFSettings(region="US", modemPreset="LONG_FAST", frequencySlot=20, hopLimit=4),
            channel=ScenarioChannel(name="Simulator", psk="AQ=="),
        )
        assert verification.region == "US"
        assert verification.channel_name == "Simulator"
    finally:
        pub.unsubscribe(on_receive, "meshtastic.receive.text")
        for interface in (internal, first, second):
            if interface is not None:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(interface.close)
        await gateway.stop()
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "rm", "--force", container_name],
            check=False,
            capture_output=True,
            text=True,
        )

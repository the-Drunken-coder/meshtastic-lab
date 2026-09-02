from __future__ import annotations

import asyncio
import contextlib
import os
import queue
import subprocess
from pathlib import Path
from typing import NoReturn

import pytest
from meshtastic import tcp_interface
from pubsub import pub

from backend.app.models import DirectedLink, Scenario, default_scenario
from backend.tests.integration.native_cluster import NativeDockerCluster


def load_scenario(name: str) -> Scenario:
    return Scenario.model_validate_json(Path("scenarios", name).read_text(encoding="utf-8"))


async def connect(cluster: NativeDockerCluster, node_id: str) -> tcp_interface.TCPInterface:
    return await asyncio.to_thread(
        tcp_interface.TCPInterface,
        hostname="127.0.0.1",
        portNumber=cluster.public_port(node_id),
        timeout=30,
    )


def fail_with_rf_trace(cluster: NativeDockerCluster, expected: str) -> NoReturn:
    trace = "\n".join(
        f"{event.sequence}: {event.event_type} {event.transmitter}->{event.receiver_set or event.receiver} "
        f"dest={event.intended_destination} packet={event.mesh_packet_id} "
        f"hops={event.hop_limit}/{event.hop_start} {event.result}"
        for event in cluster.event_broker.recent(limit=100)
    )
    verification = ", ".join(
        f"{node_id}=!{item.node_number:08x} "
        f"gateway={cluster.gateways[node_id].state}/"
        f"external={cluster.gateways[node_id].external_connected}/"
        f"rejected={cluster.gateways[node_id].rejected_clients}"
        for node_id, item in cluster.verifications.items()
    )
    gateway_trace = "\n".join(
        f"{event.node_id}: {event.kind}: {event.detail}" for event in cluster.gateway_events[-50:]
    )
    pytest.fail(
        f"did not receive {expected!r}; verified {verification}; "
        f"gateway trace:\n{gateway_trace}\nRF trace:\n{trace}\n"
        f"daemon logs:\n{cluster.daemon_log_tail()}"
    )


async def receive_text(
    cluster: NativeDockerCluster,
    received: queue.Queue[str],
    expected: str,
    timeout_seconds: float,
) -> str:
    try:
        return await asyncio.to_thread(received.get, True, timeout_seconds)
    except queue.Empty:
        fail_with_rf_trace(cluster, expected)


async def send_until_received(
    cluster: NativeDockerCluster,
    interface: tcp_interface.TCPInterface,
    received: queue.Queue[str],
    expected: str,
    destination_id: int,
    *,
    want_ack: bool = False,
    attempts: int = 3,
    attempt_timeout_seconds: float = 15,
) -> None:
    """Allow native collisions to drop attempts without weakening the delivery proof."""
    for _attempt in range(attempts):
        await asyncio.to_thread(
            interface.sendText,
            expected,
            destinationId=destination_id,
            wantAck=want_ack,
        )
        try:
            while True:
                value = await asyncio.to_thread(
                    received.get,
                    True,
                    attempt_timeout_seconds,
                )
                if value == expected:
                    return
        except queue.Empty:
            continue
    fail_with_rf_trace(cluster, expected)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_clients_on_different_nodes_broadcast_and_direct() -> None:
    cluster = NativeDockerCluster(default_scenario(2))
    first: tcp_interface.TCPInterface | None = None
    second: tcp_interface.TCPInterface | None = None
    received: queue.Queue[str] = queue.Queue()

    def on_text(packet: dict[str, object], interface: object) -> None:
        decoded = packet.get("decoded")
        if interface is second and isinstance(decoded, dict):
            text = decoded.get("text")
            if isinstance(text, str):
                received.put(text)

    pub.subscribe(on_text, "meshtastic.receive.text")
    try:
        await cluster.start()
        first, second = await asyncio.gather(connect(cluster, "node-1"), connect(cluster, "node-2"))
        assert first.myInfo is not None and second.myInfo is not None

        await asyncio.to_thread(first.sendText, "native-broadcast", wantAck=False)
        assert await receive_text(cluster, received, "native-broadcast", 20) == "native-broadcast"

        await send_until_received(
            cluster,
            first,
            received,
            "native-direct",
            cluster.verifications["node-2"].node_number,
            want_ack=True,
            attempt_timeout_seconds=20,
        )
    finally:
        pub.unsubscribe(on_text, "meshtastic.receive.text")
        for interface in (first, second):
            if interface is not None:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(interface.close)
        await cluster.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_three_node_firmware_relay_and_runtime_link_changes() -> None:
    cluster = NativeDockerCluster(load_scenario("three-node-relay.json"))
    source: tcp_interface.TCPInterface | None = None
    destination: tcp_interface.TCPInterface | None = None
    received: queue.Queue[str] = queue.Queue()

    def on_text(packet: dict[str, object], interface: object) -> None:
        decoded = packet.get("decoded")
        if interface is destination and isinstance(decoded, dict):
            text = decoded.get("text")
            if isinstance(text, str):
                received.put(text)

    pub.subscribe(on_text, "meshtastic.receive.text")
    try:
        await cluster.start()
        source, destination = await asyncio.gather(
            connect(cluster, "node-1"), connect(cluster, "node-3")
        )
        target = cluster.verifications["node-3"].node_number

        await send_until_received(cluster, source, received, "relay-one", target)

        if cluster.medium is None:
            raise AssertionError("medium did not start")
        await cluster.medium.update_link(
            DirectedLink(**{"from": "node-2", "to": "node-3", "enabled": False})
        )
        await asyncio.to_thread(source.sendText, "relay-blocked", destinationId=target, wantAck=False)
        with pytest.raises(queue.Empty):
            await asyncio.to_thread(received.get, True, 6)

        await cluster.medium.update_link(
            DirectedLink(**{"from": "node-2", "to": "node-3", "enabled": True})
        )
        await send_until_received(cluster, source, received, "relay-restored", target)
    finally:
        pub.unsubscribe(on_text, "meshtastic.receive.text")
        for interface in (source, destination):
            if interface is not None:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(interface.close)
        await cluster.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hidden_terminal_uses_native_collision_handling() -> None:
    expected_image = "meshtastic-lab:0.1.0"
    if os.environ.get("MESHTASTIC_TEST_FIRMWARE_IMAGE") != expected_image:
        pytest.skip(f"set MESHTASTIC_TEST_FIRMWARE_IMAGE={expected_image} for collision proof")

    cluster = NativeDockerCluster(load_scenario("hidden-terminal.json"))
    first: tcp_interface.TCPInterface | None = None
    third: tcp_interface.TCPInterface | None = None
    collision_log = "Collision detected, dropping current and previous packet!"
    try:
        await cluster.start()
        first, third = await asyncio.gather(connect(cluster, "node-1"), connect(cluster, "node-3"))
        for attempt in range(8):
            payload = f"collision-{attempt}-" + "x" * 180
            await asyncio.gather(
                asyncio.to_thread(first.sendText, payload, wantAck=False),
                asyncio.to_thread(third.sendText, payload, wantAck=False),
            )
            await asyncio.sleep(1)
            logs = await asyncio.to_thread(cluster.daemon_log_tail)
            if collision_log in logs:
                break
        else:
            pytest.fail(
                "hidden transmitters did not produce the native collision log after eight "
                f"controlled attempts\n{cluster.daemon_log_tail()}"
            )

        receiver_logs = await asyncio.to_thread(
            subprocess.run,
            ["docker", "logs", cluster.container_by_node["node-2"]],
            check=False,
            capture_output=True,
            text=True,
        )
        assert collision_log in receiver_logs.stdout + receiver_logs.stderr
    finally:
        for interface in (first, third):
            if interface is not None:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(interface.close)
        await cluster.stop()

#!/usr/bin/env python3
"""Headless real-firmware acceptance for the Dockerized Meshtastic Lab."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import queue
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from meshtastic import tcp_interface
from pubsub import pub

ROOT = Path(__file__).resolve().parents[1]


class AcceptanceFailure(RuntimeError):
    pass


async def wait_for_state(
    client: httpx.AsyncClient, expected: str, deadline_seconds: float
) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = await client.get("/api/state")
        response.raise_for_status()
        last = response.json()
        if last.get("state") == expected:
            return last
        if last.get("state") == "FAILED":
            raise AcceptanceFailure(f"simulation failed: {last.get('message')}")
        await asyncio.sleep(0.5)
    raise AcceptanceFailure(f"timed out waiting for {expected}; last state: {last}")


async def wait_for_api(client: httpx.AsyncClient, deadline_seconds: float) -> None:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        with contextlib.suppress(httpx.HTTPError):
            response = await client.get("/api/health")
            if response.status_code == 200:
                return
        await asyncio.sleep(1)
    raise AcceptanceFailure("API health endpoint did not become available")


async def connect(port: int) -> tcp_interface.TCPInterface:
    return await asyncio.to_thread(
        tcp_interface.TCPInterface,
        hostname="127.0.0.1",
        portNumber=port,
        timeout=30,
    )


async def receive_expected(
    received: queue.Queue[str], expected: str, deadline_seconds: float
) -> None:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        remaining = max(0.01, deadline - time.monotonic())
        try:
            value = await asyncio.to_thread(received.get, True, remaining)
        except queue.Empty as exc:
            raise AcceptanceFailure(
                f"did not receive {expected!r} within {deadline_seconds}s"
            ) from exc
        if value == expected:
            return
    raise AcceptanceFailure(f"did not receive {expected!r} within {deadline_seconds}s")


async def send_until_received(
    source: tcp_interface.TCPInterface,
    received: queue.Queue[str],
    expected: str,
    destination_id: int,
    *,
    attempts: int = 3,
    attempt_timeout_seconds: float = 15,
) -> None:
    """Retry a native RF send when an attempt is lost to modeled contention."""
    for _attempt in range(attempts):
        await asyncio.to_thread(
            source.sendText,
            expected,
            destinationId=destination_id,
            wantAck=False,
        )
        try:
            await receive_expected(received, expected, attempt_timeout_seconds)
        except AcceptanceFailure:
            continue
        return
    raise AcceptanceFailure(
        f"did not receive {expected!r} after {attempts} native RF attempts"
    )


async def assert_not_received(
    received: queue.Queue[str], expected: str, deadline_seconds: float
) -> None:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        try:
            value = await asyncio.to_thread(received.get, True, max(0.01, deadline - time.monotonic()))
        except queue.Empty:
            return
        if value == expected:
            raise AcceptanceFailure(f"unexpectedly received {expected!r} across the disabled relay link")


def listeners_closed(ports: list[int]) -> None:
    occupied: list[int] = []
    for port in ports:
        try:
            probe = socket.create_connection(("127.0.0.1", port), timeout=0.5)
        except OSError:
            continue
        with probe:
            probe.settimeout(0.5)
            try:
                probe.sendall(b"\x94\xc3\x00\x00")
                if probe.recv(1) != b"":
                    occupied.append(port)
            except TimeoutError:
                # Docker can accept a published-port connection briefly after
                # the container listener closes, but then confirms that close
                # with EOF or a reset. A timeout confirms neither and therefore
                # still represents a live, silent gateway listener.
                occupied.append(port)
            except OSError:
                continue
    if occupied:
        raise AcceptanceFailure(f"public listeners remained open after stop: {occupied}")


async def run(base_url: str, *, start_stack: bool) -> dict[str, Any]:
    stack_owned = False
    step = "initialize"
    interfaces: list[tcp_interface.TCPInterface] = []
    public_ports: list[int] = []
    received: queue.Queue[str] = queue.Queue()
    destination: tcp_interface.TCPInterface | None = None

    def on_text(packet: dict[str, object], interface: object) -> None:
        decoded = packet.get("decoded")
        if interface is destination and isinstance(decoded, dict):
            text = decoded.get("text")
            if isinstance(text, str):
                received.put(text)

    timeout = httpx.Timeout(200, connect=10)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        try:
            step = "wait for API health"
            try:
                await wait_for_api(client, 2)
            except AcceptanceFailure:
                if not start_stack:
                    raise
                await asyncio.to_thread(
                    subprocess.run,
                    ["docker", "compose", "up", "--build", "--detach"],
                    cwd=ROOT,
                    check=True,
                )
                stack_owned = True
                await wait_for_api(client, 180)

            capabilities = (await client.get("/api/capabilities")).json()
            if not capabilities.get("collisionAvailable"):
                raise AcceptanceFailure(f"native collision capability unavailable: {capabilities}")
            provenance_fields = (
                "firmwareCommit",
                "collisionPatchSha256",
                "firmwareBinarySha256",
                "buildArchitecture",
                "clientLibraryVersion",
                "upstreamBaseImageDigest",
            )
            if not capabilities.get("provenanceAvailable") or any(
                capabilities.get(field) in {None, "", "unavailable"}
                for field in provenance_fields
            ):
                raise AcceptanceFailure(f"native build provenance unavailable: {capabilities}")

            step = "load and start three-node relay scenario"
            await client.post("/api/simulation/stop")
            await wait_for_state(client, "STOPPED", 30)
            scenario = json.loads((ROOT / "scenarios" / "three-node-relay.json").read_text())
            replaced = await client.put("/api/scenario", json=scenario)
            replaced.raise_for_status()
            started = await client.post("/api/simulation/start")
            started.raise_for_status()
            await wait_for_state(client, "RUNNING", 180)

            step = "verify API node information"
            node_views = (await client.get("/api/nodes")).json()
            if len(node_views) != 3 or any(node.get("nodeNumber") is None for node in node_views):
                raise AcceptanceFailure(f"node information was not verified: {node_views}")
            public_ports = [
                int(str(node["publicEndpoint"]).rsplit(":", 1)[1]) for node in node_views
            ]

            step = "connect three official clients"
            interfaces = list(await asyncio.gather(*(connect(port) for port in public_ports)))
            if any(interface.myInfo is None for interface in interfaces):
                raise AcceptanceFailure("an official client did not receive local node information")
            source, _, destination = interfaces
            pub.subscribe(on_text, "meshtastic.receive.text")
            target_number = next(
                int(node["nodeNumber"]) for node in node_views if node["id"] == "node-3"
            )

            step = "deliver through firmware relay"
            await send_until_received(
                source,
                received,
                "accept-relay-one",
                target_number,
            )

            step = "disable relay link and confirm bounded non-delivery"
            link = {"from": "node-2", "to": "node-3", "enabled": False, "rssiDbm": -85, "snrDb": 8}
            disabled = await client.put("/api/links", json=link)
            disabled.raise_for_status()
            while not received.empty():
                with contextlib.suppress(queue.Empty):
                    received.get_nowait()
            await asyncio.to_thread(
                source.sendText, "accept-relay-blocked", destinationId=target_number, wantAck=False
            )
            await assert_not_received(received, "accept-relay-blocked", 7)

            step = "restore relay link and confirm delivery"
            link["enabled"] = True
            restored = await client.put("/api/links", json=link)
            restored.raise_for_status()
            await send_until_received(
                source,
                received,
                "accept-relay-restored",
                target_number,
            )

            step = "run fixed-rate traffic"
            traffic_request = {
                "kind": "direct-text",
                "sourceNodes": ["node-1"],
                "destinationStrategy": "fixed",
                "fixedDestination": "node-3",
                "messagesPerMinute": 30,
                "payloadBytes": 64,
                "durationSeconds": 8,
                "acknowledgmentRequested": True,
                "seed": 17,
            }
            traffic_started = await client.post("/api/traffic/runs", json=traffic_request)
            traffic_started.raise_for_status()
            run_id = traffic_started.json()["runId"]
            deadline = time.monotonic() + 330
            result: dict[str, Any] = {}
            while time.monotonic() < deadline:
                result = (await client.get("/api/traffic/runs/current")).json()
                if result.get("state") in {"COMPLETED", "FAILED", "CANCELLED"}:
                    break
                await asyncio.sleep(0.5)
            step = "validate persisted traffic metrics"
            if result.get("state") != "COMPLETED":
                raise AcceptanceFailure(f"traffic run did not complete: {result}")
            metrics = result["metrics"]
            required_positive = {
                "generatedApplicationMessages": metrics["generatedApplicationMessages"],
                "uniqueApplicationMessagesDelivered": metrics["uniqueApplicationMessagesDelivered"],
                "rfTransmissions": metrics["rfTransmissions"],
                "observedAirtimeMs": metrics["observedAirtimeMs"],
                "medianLatencyMs": metrics["medianLatencyMs"],
            }
            if any(value is None or value <= 0 for value in required_positive.values()):
                raise AcceptanceFailure(f"traffic metrics were not internally useful: {metrics}")
            if metrics["rfTransmissions"] < result["transmitted"]:
                raise AcceptanceFailure("RF transmission count is below transmitted application count")
            persisted = await client.get(f"/api/traffic/runs/{run_id}")
            persisted.raise_for_status()
            if persisted.json().get("scenarioSnapshot", {}).get("name") != "three-node-relay":
                raise AcceptanceFailure("persisted run did not contain the exact scenario snapshot")
            if "generatedMessages" in persisted.json():
                raise AcceptanceFailure("bounded result endpoint exposed generated message records")
            exported = await client.get(f"/api/traffic/runs/{run_id}/export")
            exported.raise_for_status()
            export_result = exported.json()
            if not export_result.get("generatedMessages"):
                raise AcceptanceFailure("completed export omitted generated message records")
            for field in provenance_fields:
                if export_result.get(field) != capabilities.get(field):
                    raise AcceptanceFailure(
                        f"completed export provenance differs for {field}: "
                        f"{export_result.get(field)} != {capabilities.get(field)}"
                    )

            return {
                "simulation": "three-node-relay",
                "officialClients": 3,
                "relayBlockedAndRestored": True,
                "trafficRunId": run_id,
                "metrics": required_positive,
                "collisionModel": capabilities["collisionModel"],
            }
        except Exception as exc:
            raise AcceptanceFailure(f"{step}: {type(exc).__name__}: {exc}") from exc
        finally:
            original_failure = sys.exc_info()[0] is not None
            with contextlib.suppress(Exception):
                pub.unsubscribe(on_text, "meshtastic.receive.text")
            for interface in interfaces:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(interface.close)
            cleanup_error: Exception | None = None
            try:
                await client.post("/api/simulation/stop")
                await wait_for_state(client, "STOPPED", 30)
                listeners_closed(public_ports)
            except Exception as exc:
                cleanup_error = exc
            if stack_owned:
                await asyncio.to_thread(
                    subprocess.run, ["docker", "compose", "down"], cwd=ROOT, check=False
                )
            if cleanup_error is not None and not original_failure:
                raise cleanup_error


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument(
        "--no-start-stack",
        action="store_true",
        help="fail instead of starting Docker Compose when the API is unavailable",
    )
    arguments = parser.parse_args()
    try:
        summary = asyncio.run(run(arguments.base_url, start_stack=not arguments.no_start_stack))
    except Exception as exc:
        print(f"ACCEPTANCE FAILED: {exc}", file=sys.stderr)
        return 1
    print("ACCEPTANCE PASSED")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

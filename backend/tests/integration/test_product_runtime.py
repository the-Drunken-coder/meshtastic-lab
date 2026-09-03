from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import subprocess
import time
import uuid
from pathlib import Path

import httpx
import pytest

from backend.app.models import default_scenario

PRODUCT_IMAGE = "meshtastic-lab:0.1.0"
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
BOUNDED_BROADCAST_COMPLETION_SECONDS = 60
BOUNDED_TRAFFIC_COMPLETION_SECONDS = 330


def unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def public_listener_is_closed(port: int) -> bool:
    """Account for Docker Desktop's proxy accepting before it discovers no backend."""
    try:
        probe = socket.create_connection(("127.0.0.1", port), timeout=0.5)
    except OSError:
        return True
    with probe:
        probe.settimeout(0.5)
        try:
            probe.sendall(b"\x94\xc3\x00\x00")
            return probe.recv(1) == b""
        except OSError:
            return True


async def wait_for_state(
    client: httpx.AsyncClient,
    expected: str,
    deadline_seconds: float,
) -> dict[str, object]:
    deadline = time.monotonic() + deadline_seconds
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        response = await client.get("/api/state")
        response.raise_for_status()
        last = response.json()
        if last.get("state") == expected:
            return last
        await asyncio.sleep(0.25)
    pytest.fail(f"timed out waiting for {expected}: {last}")


async def wait_for_traffic(client: httpx.AsyncClient, deadline_seconds: float) -> dict[str, object]:
    deadline = time.monotonic() + deadline_seconds
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        response = await client.get("/api/traffic/runs/current")
        response.raise_for_status()
        last = response.json()
        if last.get("state") in {"COMPLETED", "FAILED", "CANCELLED"}:
            return last
        await asyncio.sleep(0.25)
    pytest.fail(f"traffic did not finish: {last}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_product_lifecycle_failure_and_five_node_cleanup() -> None:
    image = os.environ.get("MESHTASTIC_PRODUCT_IMAGE", PRODUCT_IMAGE)
    inspected = await asyncio.to_thread(
        subprocess.run,
        ["docker", "image", "inspect", image],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspected.returncode != 0:
        pytest.skip(f"build {image} before running product runtime integration")

    name = f"ml-product-{uuid.uuid4().hex[:8]}"
    web_port = unused_port()
    node_ports = [unused_port() for _ in range(10)]
    command = [
        "docker",
        "run",
        "--detach",
        "--rm",
        "--name",
        name,
        "--publish",
        f"127.0.0.1:{web_port}:8080",
    ]
    for index, host_port in enumerate(node_ports, start=1):
        command.extend(["--publish", f"127.0.0.1:{host_port}:{45000 + index}"])
    command.append(image)
    started = await asyncio.to_thread(
        subprocess.run, command, check=True, capture_output=True, text=True
    )
    assert started.stdout.strip()

    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{web_port}", timeout=180
        ) as client:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with contextlib.suppress(httpx.HTTPError):
                    if (await client.get("/api/health")).status_code == 200:
                        break
                await asyncio.sleep(0.25)
            else:
                pytest.fail("product API did not become healthy")

            two_nodes = default_scenario(2).model_dump(mode="json", by_alias=True)
            assert (await client.put("/api/scenario", json=two_nodes)).status_code == 200
            for cycle in range(3):
                response = await client.post("/api/simulation/start")
                if response.is_error:
                    state = (await client.get("/api/state")).text
                    logs = await asyncio.to_thread(
                        subprocess.run,
                        ["docker", "logs", "--tail", "250", name],
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    pytest.fail(
                        f"start cycle {cycle + 1} failed: {response.text}; state={state}; "
                        f"logs={logs.stdout}{logs.stderr}"
                    )
                response.raise_for_status()
                await wait_for_state(client, "RUNNING", 120)
                response = await client.post("/api/simulation/stop")
                response.raise_for_status()
                await wait_for_state(client, "STOPPED", 30)
                assert await asyncio.to_thread(public_listener_is_closed, node_ports[0])

            response = await client.post("/api/simulation/start")
            if response.is_error:
                state = (await client.get("/api/state")).text
                logs = await asyncio.to_thread(
                    subprocess.run,
                    ["docker", "logs", "--tail", "250", name],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                pytest.fail(
                    f"fourth start failed: {response.text}; state={state}; "
                    f"logs={logs.stdout}{logs.stderr}"
                )
            response.raise_for_status()
            await wait_for_state(client, "RUNNING", 120)
            nodes = (await client.get("/api/nodes")).json()
            victim = nodes[0]
            killed = await asyncio.to_thread(
                subprocess.run,
                ["docker", "exec", name, "sh", "-c", f"kill -9 {victim['processId']}"],
                check=True,
                capture_output=True,
                text=True,
            )
            assert killed.returncode == 0
            failed = await wait_for_state(client, "FAILED", 15)
            assert victim["id"] in str(failed.get("message"))
            await client.post("/api/simulation/stop")
            await wait_for_state(client, "STOPPED", 30)
            archived_logs = await client.get(f"/api/nodes/{victim['id']}/logs?stream=stderr")
            archived_logs.raise_for_status()

            hidden_terminal_path = REPOSITORY_ROOT / "scenarios" / "hidden-terminal.json"
            hidden_terminal = json.loads(
                await asyncio.to_thread(hidden_terminal_path.read_text, encoding="utf-8")
            )
            assert (await client.put("/api/scenario", json=hidden_terminal)).status_code == 200
            response = await client.post("/api/simulation/start")
            response.raise_for_status()
            await wait_for_state(client, "RUNNING", 120)
            collision_run = await client.post(
                "/api/traffic/runs",
                json={
                    "kind": "broadcast-text",
                    "sourceNodes": ["node-1", "node-3"],
                    "destinationStrategy": "fixed",
                    "messagesPerMinute": 600,
                    "payloadBytes": 200,
                    "durationSeconds": 8,
                    "acknowledgmentRequested": False,
                    "seed": 7,
                },
            )
            collision_run.raise_for_status()
            collision_result = await wait_for_traffic(
                client, BOUNDED_BROADCAST_COMPLETION_SECONDS
            )
            assert collision_result["state"] == "COMPLETED"
            assert collision_result["submissionFailed"] > 0
            assert collision_result["requested"] == (
                collision_result["submitted"] + collision_result["submissionFailed"]
            )
            collision_metrics = collision_result["metrics"]
            assert isinstance(collision_metrics, dict)
            assert collision_metrics["failedReceptions"] > 0
            response = await client.post("/api/simulation/stop")
            response.raise_for_status()
            await wait_for_state(client, "STOPPED", 30)

            five_nodes = default_scenario(5).model_dump(mode="json", by_alias=True)
            assert (await client.put("/api/scenario", json=five_nodes)).status_code == 200
            response = await client.post("/api/simulation/start")
            if response.is_error:
                state = (await client.get("/api/state")).text
                logs = await asyncio.to_thread(
                    subprocess.run,
                    ["docker", "logs", "--tail", "250", name],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                pytest.fail(
                    f"five-node start failed: {response.text}; state={state}; "
                    f"logs={logs.stdout}{logs.stderr}"
                )
            response.raise_for_status()
            await wait_for_state(client, "RUNNING", 180)
            traffic_request = {
                "kind": "direct-text",
                "sourceNodes": ["node-1"],
                "destinationStrategy": "fixed",
                "fixedDestination": "node-5",
                "messagesPerMinute": 30,
                "payloadBytes": 64,
                "durationSeconds": 8,
                "acknowledgmentRequested": True,
                "seed": 9,
            }
            response = await client.post("/api/traffic/runs", json=traffic_request)
            response.raise_for_status()
            # The controller may wait for the pinned firmware's complete retry horizon
            # before it freezes an unresolved accepted message.
            result = await wait_for_traffic(client, BOUNDED_TRAFFIC_COMPLETION_SECONDS)
            assert result["state"] == "COMPLETED", result.get("failure")
            metrics = result["metrics"]
            assert isinstance(metrics, dict)
            assert metrics["generatedApplicationMessages"] > 0
            assert metrics["uniqueApplicationMessagesDelivered"] > 0
            assert metrics["rfTransmissions"] > 0
            assert metrics["observedAirtimeMs"] > 0

            traffic_request["durationSeconds"] = 60
            active = await client.post("/api/traffic/runs", json=traffic_request)
            active.raise_for_status()
            active_run_id = active.json()["runId"]
            await asyncio.sleep(1)
            active_scenario = (await client.get("/api/scenario")).json()
            active_link = next(
                link
                for link in active_scenario["links"]
                if link["from"] == "node-1" and link["to"] == "node-5"
            )
            disabled_link = {**active_link, "enabled": False}
            changed = await client.put("/api/links", json=disabled_link)
            changed.raise_for_status()
            restored = await client.put("/api/links", json=active_link)
            restored.raise_for_status()
            stop_started = time.monotonic()
            stopped = await client.post("/api/simulation/stop")
            stopped.raise_for_status()
            assert time.monotonic() - stop_started < 20
            await wait_for_state(client, "STOPPED", 5)
            cancelled = await client.get(f"/api/traffic/runs/{active_run_id}")
            cancelled.raise_for_status()
            assert cancelled.json()["state"] == "CANCELLED"
            exported = await client.get(f"/api/traffic/runs/{active_run_id}/export")
            exported.raise_for_status()
            changes = exported.json()["topologyChanges"]
            assert [change["link"]["enabled"] for change in changes] == [False, True]
            assert changes[0]["eventSequence"] < changes[1]["eventSequence"]
    finally:
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "rm", "--force", name],
            check=False,
            capture_output=True,
            text=True,
        )

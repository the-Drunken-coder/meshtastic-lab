import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app.main import create_app
from backend.app.metrics import EventBroker
from backend.app.models import default_scenario
from backend.app.runtime import ProcessRecord
from backend.app.simulator import LifecycleState, SimulatorService
from backend.app.traffic import TrafficController, TrafficRunRequest


def test_health_scenario_and_structured_collision_failure(tmp_path: Path) -> None:
    service = SimulatorService(
        binary_path=tmp_path / "missing-meshtasticd",
        data_root=tmp_path / "data",
        collision_marker=tmp_path / "missing-marker",
    )
    with TestClient(create_app(service)) as client:
        health = client.get("/api/health")
        scenario = client.get("/api/scenario")
        start = client.post("/api/simulation/start")

    assert health.json() == {"status": "ok", "ready": False, "lifecycle": "STOPPED"}
    assert scenario.json()["nodeCount"] == 5
    assert start.status_code == 409
    assert start.json()["error"]["code"] == "NATIVE_COLLISION_UNAVAILABLE"


def test_scenario_can_change_only_while_stopped(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, collision_marker=tmp_path / "marker")
    app = create_app(service)
    with TestClient(app) as client:
        data = client.get("/api/scenario").json()
        data["name"] = "edited"
        replaced = client.put("/api/scenario", json=data)

    assert replaced.status_code == 200
    assert replaced.json()["name"] == "edited"


def test_incomplete_scenario_link_matrix_returns_validation_error(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, collision_marker=tmp_path / "marker")
    app = create_app(service)
    with TestClient(app) as client:
        data = client.get("/api/scenario").json()
        data["links"].pop()
        response = client.put("/api/scenario", json=data)

    assert response.status_code == 422
    assert "missing directed links" in response.text


def test_openapi_describes_bounded_traffic_responses(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, collision_marker=tmp_path / "marker")
    schema = create_app(service).openapi()

    current = schema["paths"]["/api/traffic/runs/current"]["get"]["responses"]["200"]
    result = schema["paths"]["/api/traffic/runs/{run_id}"]["get"]["responses"]["200"]
    assert current["content"]["application/json"]["schema"] != {}
    assert result["content"]["application/json"]["schema"]["$ref"].endswith(
        "/TrafficRunSummary"
    )


def test_daemon_logs_remain_available_after_process_cleanup(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, collision_marker=tmp_path / "marker")
    record = ProcessRecord(
        node_id="node-1",
        hardware_id=1,
        internal_port=46001,
        data_directory=tmp_path / "node-1" / "state",
        stdout_path=tmp_path / "node-1" / "stdout.log",
        stderr_path=tmp_path / "node-1" / "stderr.log",
    )
    record.stderr_lines.append("native failure")
    service.supervisor.records[record.node_id] = record
    asyncio.run(service.supervisor.stop())

    with TestClient(create_app(service)) as client:
        response = client.get("/api/nodes/node-1/logs?stream=stderr")

    assert response.status_code == 200
    assert response.json()["lines"] == ["native failure"]


def test_unpersisted_terminal_result_survives_cleanup_and_exports(tmp_path: Path) -> None:
    service = SimulatorService(data_root=tmp_path, collision_marker=tmp_path / "marker")
    service.results_root.write_text("not a directory", encoding="utf-8")
    scenario = default_scenario(2)

    class Gateway:
        async def send_to_radio(self, _message: object, *, source: str) -> None:
            del source

    service.traffic = TrafficController(
        scenario=scenario,
        gateways={node.id: Gateway() for node in scenario.nodes},  # type: ignore[arg-type]
        hardware_ids={"node-1": 1, "node-2": 2},
        event_broker=EventBroker(),
        results_root=service.results_root,
        settle_seconds=0,
    )

    async def finish_replace_and_cleanup() -> tuple[str, str]:
        if service.traffic is None:
            raise AssertionError("traffic controller missing")
        service.state = LifecycleState.RUNNING

        async def sample_local_stats() -> dict[str, int]:
            return {"node-1": 0, "node-2": 0}

        service._sample_local_stats = sample_local_stats  # type: ignore[method-assign]
        request = TrafficRunRequest(
            sourceNodes=["node-1"],
            messagesPerMinute=600,
            durationSeconds=0.01,
            payloadBytes=64,
        )
        first_run_id = await service.start_traffic(request)
        first = await service.traffic.wait(deadline_seconds=2)
        assert first.failure is not None and "result persistence failed" in first.failure

        second_run_id = await service.start_traffic(request)
        assert service.traffic_result(first_run_id).run_id == first_run_id
        second = await service.traffic.wait(deadline_seconds=2)
        assert second.failure is not None and "result persistence failed" in second.failure
        await service._cleanup_resources()
        service.state = LifecycleState.STOPPED
        return first_run_id, second_run_id

    first_run_id, second_run_id = asyncio.run(finish_replace_and_cleanup())
    assert service.traffic is None
    assert set(service.completed_runs()) == {first_run_id, second_run_id}

    with TestClient(create_app(service)) as client:
        summary = client.get(f"/api/traffic/runs/{first_run_id}")
        exported = client.get(f"/api/traffic/runs/{first_run_id}/export")

    assert summary.status_code == 200
    assert summary.json()["state"] == "FAILED"
    assert exported.status_code == 200
    assert exported.json()["runId"] == first_run_id

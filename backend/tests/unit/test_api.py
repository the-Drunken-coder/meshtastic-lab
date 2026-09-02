from pathlib import Path

from fastapi.testclient import TestClient

from backend.app.main import create_app
from backend.app.simulator import SimulatorService


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

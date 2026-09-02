"""FastAPI entry point for Meshtastic Lab."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from backend.app.logging_config import configure_logging
from backend.app.metrics import EventType
from backend.app.models import DirectedLink, Scenario, TopologyPreset
from backend.app.simulator import SimulationConflict, SimulatorService
from backend.app.traffic import TrafficRunRequest, TrafficRunState, TrafficRunSummary


class TopologyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preset: TopologyPreset


class TrafficStarted(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(alias="runId")
    state: TrafficRunState


class IdleTraffic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state: Literal[TrafficRunState.IDLE] = TrafficRunState.IDLE


class HealthView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    ready: bool
    lifecycle: str


def create_app(service: SimulatorService | None = None) -> FastAPI:
    simulator = service or SimulatorService()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.simulator = simulator
        yield
        if simulator.state.value != "STOPPED":
            await simulator.stop()

    app = FastAPI(
        title="Meshtastic Lab API",
        version="0.1.0",
        description="Firmware-in-the-loop Meshtastic network simulation API",
        lifespan=lifespan,
    )
    app.state.simulator = simulator

    @app.exception_handler(SimulationConflict)
    async def simulation_conflict_handler(_request: Request, exc: SimulationConflict) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": {"code": exc.code, "message": str(exc)}},
        )

    @app.exception_handler(FileNotFoundError)
    async def not_found_handler(request: Request, exc: FileNotFoundError) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=404,
            content={"error": {"code": "NOT_FOUND", "message": str(exc)}},
        )

    @app.get("/api/health", response_model=HealthView)
    async def health() -> HealthView:
        return HealthView(
            status="ok",
            ready=simulator.state.value == "RUNNING",
            lifecycle=simulator.state.value,
        )

    @app.get("/api/capabilities")
    async def capabilities() -> object:
        return simulator.capabilities()

    @app.get("/api/state")
    async def state() -> object:
        return simulator.lifecycle()

    @app.get("/api/scenario", response_model=Scenario)
    async def scenario() -> Scenario:
        return simulator.scenario

    @app.put("/api/scenario", response_model=Scenario)
    async def replace_scenario(scenario_update: Scenario) -> Scenario:
        return simulator.replace_scenario(scenario_update)

    @app.get("/api/scenario/export")
    async def export_scenario() -> JSONResponse:
        return JSONResponse(
            content=simulator.scenario.model_dump(mode="json", by_alias=True),
            headers={
                "Content-Disposition": f'attachment; filename="{simulator.scenario.name}.json"'
            },
        )

    @app.post("/api/simulation/start")
    async def start() -> object:
        return await simulator.start()

    @app.post("/api/simulation/stop")
    async def stop() -> object:
        return await simulator.stop()

    @app.post("/api/simulation/reset")
    async def reset() -> object:
        return await simulator.reset()

    @app.get("/api/nodes")
    async def nodes() -> object:
        return simulator.nodes()

    @app.get("/api/nodes/{node_id}/logs")
    async def node_logs(
        node_id: str,
        stream: Annotated[str, Query(pattern="^(stdout|stderr)$")] = "stderr",
        limit: Annotated[int, Query(ge=1, le=250)] = 100,
    ) -> object:
        if node_id not in simulator.supervisor.records:
            raise HTTPException(status_code=404, detail=f"unknown or stopped node: {node_id}")
        return {
            "nodeId": node_id,
            "stream": stream,
            "lines": simulator.supervisor.recent_logs(node_id, stream=stream, limit=limit),
        }

    @app.put("/api/links", response_model=DirectedLink)
    async def update_link(link: DirectedLink) -> DirectedLink:
        return await simulator.update_link(link)

    @app.post("/api/topology", response_model=Scenario)
    async def apply_topology(request: TopologyRequest) -> Scenario:
        return await simulator.apply_topology(request.preset)

    @app.post("/api/traffic/runs", response_model=TrafficStarted)
    async def start_traffic(request: TrafficRunRequest) -> TrafficStarted:
        run_id = await simulator.start_traffic(request)
        return TrafficStarted(runId=run_id, state=TrafficRunState.RUNNING)

    @app.post(
        "/api/traffic/runs/stop", response_model=TrafficRunSummary | IdleTraffic
    )
    async def stop_traffic() -> TrafficRunSummary | IdleTraffic:
        await simulator.stop_traffic()
        summary = simulator.traffic.summary() if simulator.traffic is not None else None
        return summary or IdleTraffic()

    @app.get(
        "/api/traffic/runs/current", response_model=TrafficRunSummary | IdleTraffic
    )
    async def current_traffic() -> TrafficRunSummary | IdleTraffic:
        summary = simulator.traffic.summary() if simulator.traffic is not None else None
        return summary or IdleTraffic()

    @app.get("/api/traffic/runs")
    async def completed_runs() -> object:
        return {"runIds": simulator.completed_runs()}

    @app.get("/api/traffic/runs/{run_id}", response_model=TrafficRunSummary)
    async def traffic_result(run_id: str) -> TrafficRunSummary:
        return simulator.traffic_summary(run_id)

    @app.get("/api/traffic/runs/{run_id}/export")
    async def export_traffic_result(run_id: str) -> FileResponse:
        path = simulator.results_root / f"{run_id}.json"
        if not path.is_file():
            if (
                simulator.traffic is not None
                and simulator.traffic.current is not None
                and simulator.traffic.current.run_id == run_id
            ):
                raise SimulationConflict(
                    "TRAFFIC_RUN_NOT_COMPLETE", "traffic results can be exported after the run finishes"
                )
            raise FileNotFoundError(run_id)
        return FileResponse(path, media_type="application/json", filename=f"{run_id}.json")

    @app.get("/api/events")
    async def events(
        limit: Annotated[int, Query(ge=1, le=5000)] = 250,
        node_id: Annotated[str | None, Query(alias="nodeId")] = None,
        event_type: Annotated[EventType | None, Query(alias="eventType")] = None,
    ) -> object:
        return simulator.event_broker.recent(
            limit=limit, node_id=node_id, event_type=event_type
        )

    @app.websocket("/api/events/ws")
    async def event_stream(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            async with simulator.event_broker.subscribe() as subscription:
                while True:
                    event_task = asyncio.create_task(subscription.next())
                    receive_task = asyncio.create_task(websocket.receive())
                    done, pending = await asyncio.wait(
                        {event_task, receive_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    if receive_task in done:
                        received = receive_task.result()
                        if received["type"] == "websocket.disconnect":
                            return
                    if event_task in done:
                        event = event_task.result()
                        await websocket.send_json(event.model_dump(mode="json", by_alias=True))
        except WebSocketDisconnect:
            return
        except asyncio.CancelledError:
            raise

    frontend_root = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if frontend_root.is_dir():
        app.mount("/", StaticFiles(directory=frontend_root, html=True), name="frontend")

    return app


configure_logging()
app = create_app()

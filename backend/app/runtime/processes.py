"""Bounded lifecycle management for native meshtasticd children."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
from collections import deque
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from backend.app.models import Scenario

LOGGER = logging.getLogger(__name__)
ProcessFailureHandler = Callable[[str, int | None], Coroutine[object, object, None]]
OUTPUT_DRAIN_TIMEOUT_SECONDS = 1.0


class NodeProcessState(StrEnum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    FAILED = "FAILED"


@dataclass(slots=True)
class ProcessRecord:
    node_id: str
    hardware_id: int
    internal_port: int
    data_directory: Path
    stdout_path: Path
    stderr_path: Path
    state: NodeProcessState = NodeProcessState.STOPPED
    process: asyncio.subprocess.Process | None = None
    stdout_lines: deque[str] = field(default_factory=lambda: deque(maxlen=250))
    stderr_lines: deque[str] = field(default_factory=lambda: deque(maxlen=250))
    tasks: set[asyncio.Task[None]] = field(default_factory=set)

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process is not None else None


@dataclass(frozen=True, slots=True)
class ArchivedProcessLogs:
    stdout_lines: tuple[str, ...]
    stderr_lines: tuple[str, ...]


class NativeProcessSupervisor:
    """Start isolated firmware children and reap every exit path."""

    def __init__(
        self,
        *,
        binary_path: Path = Path("/usr/bin/meshtasticd"),
        data_root: Path = Path("/data/nodes"),
        internal_port_base: int = 46001,
        startup_timeout: float = 30.0,
        shutdown_timeout: float = 8.0,
        failure_handler: ProcessFailureHandler | None = None,
    ) -> None:
        self.binary_path = binary_path
        self.data_root = data_root
        self.internal_port_base = internal_port_base
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.failure_handler = failure_handler
        self.records: dict[str, ProcessRecord] = {}
        self.archived_logs: dict[str, ArchivedProcessLogs] = {}
        self._stopping = False

    async def start(self, scenario: Scenario) -> Mapping[str, ProcessRecord]:
        if self.records:
            raise RuntimeError("firmware processes are already allocated")
        self.clear_archived_logs()
        if not self.binary_path.is_file():
            raise FileNotFoundError(f"native firmware binary not found: {self.binary_path}")

        self.data_root.mkdir(parents=True, exist_ok=True)
        self._stopping = False
        try:
            for index, node in enumerate(scenario.nodes):
                record = self._record_for(node.id, index)
                self.records[node.id] = record
                await self._start_one(record, erase=scenario.fresh_state)
        except Exception:
            await self.stop()
            raise
        return self.records

    async def stop(self) -> None:
        if not self.records:
            return
        self._stopping = True
        records = tuple(self.records.values())
        for record in records:
            if record.process is not None and record.process.returncode is None:
                record.state = NodeProcessState.STOPPING
                record.process.terminate()

        try:
            async with asyncio.timeout(self.shutdown_timeout):
                await asyncio.gather(
                    *(record.process.wait() for record in records if record.process is not None),
                    return_exceptions=True,
                )
        except TimeoutError:
            for record in records:
                if record.process is not None and record.process.returncode is None:
                    LOGGER.warning("force-killing firmware child", extra={"node_id": record.node_id})
                    record.process.kill()
            await asyncio.gather(
                *(record.process.wait() for record in records if record.process is not None),
                return_exceptions=True,
            )

        for record in records:
            _, pending_tasks = await asyncio.wait(
                tuple(record.tasks), timeout=OUTPUT_DRAIN_TIMEOUT_SECONDS
            ) if record.tasks else (set(), set())
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            record.tasks.clear()
            if record.state != NodeProcessState.FAILED:
                record.state = NodeProcessState.STOPPED
            self.archived_logs[record.node_id] = ArchivedProcessLogs(
                stdout_lines=tuple(record.stdout_lines),
                stderr_lines=tuple(record.stderr_lines),
            )
        self.records.clear()
        self._stopping = False

    def recent_logs(self, node_id: str, *, stream: str = "stderr", limit: int = 100) -> list[str]:
        record = self.records.get(node_id)
        if record is not None:
            active_lines = record.stderr_lines if stream == "stderr" else record.stdout_lines
            return list(active_lines)[-limit:]
        archive = self.archived_logs[node_id]
        archived_lines = archive.stderr_lines if stream == "stderr" else archive.stdout_lines
        return list(archived_lines)[-limit:]

    def has_logs(self, node_id: str) -> bool:
        return node_id in self.records or node_id in self.archived_logs

    def clear_archived_logs(self) -> None:
        self.archived_logs.clear()

    def _record_for(self, node_id: str, index: int) -> ProcessRecord:
        node_root = self.data_root / node_id
        return ProcessRecord(
            node_id=node_id,
            hardware_id=0xA11CE001 + index,
            internal_port=self.internal_port_base + index,
            data_directory=node_root / "state",
            stdout_path=node_root / "stdout.log",
            stderr_path=node_root / "stderr.log",
        )

    async def _start_one(self, record: ProcessRecord, *, erase: bool) -> None:
        node_root = record.data_directory.parent
        if erase and node_root.exists():
            shutil.rmtree(node_root)
        record.data_directory.mkdir(parents=True, exist_ok=True)
        record.state = NodeProcessState.STARTING
        command = [
            str(self.binary_path),
            "--sim",
            "--fsdir",
            str(record.data_directory),
            "--hwid",
            str(record.hardware_id),
            "--port",
            str(record.internal_port),
        ]
        if erase:
            command.insert(1, "--erase")

        record.process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        record.state = NodeProcessState.RUNNING
        if record.process.stdout is None or record.process.stderr is None:
            raise RuntimeError(f"failed to capture output for {record.node_id}")

        self._track(
            record,
            self._drain_output(record, record.process.stdout, record.stdout_path, record.stdout_lines),
            f"{record.node_id}-stdout",
        )
        self._track(
            record,
            self._drain_output(record, record.process.stderr, record.stderr_path, record.stderr_lines),
            f"{record.node_id}-stderr",
        )
        self._track(record, self._monitor_exit(record), f"{record.node_id}-exit")

    def _track(self, record: ProcessRecord, coroutine: Coroutine[object, object, None], name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        record.tasks.add(task)
        task.add_done_callback(record.tasks.discard)

    async def _drain_output(
        self,
        record: ProcessRecord,
        stream: asyncio.StreamReader,
        path: Path,
        recent: deque[str],
    ) -> None:
        del record
        with path.open("a", encoding="utf-8") as output:
            while line := await stream.readline():
                text = line.decode("utf-8", errors="replace").rstrip()
                recent.append(text)
                output.write(text + "\n")
                output.flush()

    async def _monitor_exit(self, record: ProcessRecord) -> None:
        if record.process is None:
            return
        return_code = await record.process.wait()
        if self._stopping:
            return
        record.state = NodeProcessState.FAILED
        LOGGER.error(
            "firmware child exited",
            extra={"node_id": record.node_id, "process_id": record.pid, "return_code": return_code},
        )
        if self.failure_handler is not None:
            with contextlib.suppress(Exception):
                await self.failure_handler(record.node_id, return_code)

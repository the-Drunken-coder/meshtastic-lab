from __future__ import annotations

import asyncio
from collections import deque

import pytest

from backend.app.runtime import NativeProcessSupervisor


def test_hardware_ids_do_not_depend_on_scenario_order() -> None:
    ordered = NativeProcessSupervisor._hardware_ids_for(["alpha", "bravo", "charlie"])
    reordered = NativeProcessSupervisor._hardware_ids_for(["charlie", "alpha", "bravo"])

    assert reordered == ordered
    assert len(set(ordered.values())) == len(ordered)
    assert all(hardware_id not in {0, 0xFFFFFFFF} for hardware_id in ordered.values())


@pytest.mark.asyncio
async def test_log_storage_failure_keeps_draining_process_output(tmp_path) -> None:
    supervisor = NativeProcessSupervisor(data_root=tmp_path)
    stream = asyncio.StreamReader()
    stream.feed_data(b"first\nsecond\n")
    stream.feed_eof()
    recent: deque[str] = deque(maxlen=10)
    invalid_log_path = tmp_path / "directory-instead-of-log"
    invalid_log_path.mkdir()

    await supervisor._drain_output(stream, invalid_log_path, recent)

    assert list(recent) == ["first", "second"]

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


@pytest.mark.asyncio
async def test_persisted_logs_rotate_with_a_bounded_backup(tmp_path) -> None:
    supervisor = NativeProcessSupervisor(data_root=tmp_path, log_max_bytes=10)
    first_stream = asyncio.StreamReader()
    first_stream.feed_data(b"1111\n2222\n")
    first_stream.feed_eof()
    recent: deque[str] = deque(maxlen=10)
    path = tmp_path / "stdout.log"

    await supervisor._drain_output(first_stream, path, recent)
    second_stream = asyncio.StreamReader()
    second_stream.feed_data(b"3333\n")
    second_stream.feed_eof()
    await supervisor._drain_output(second_stream, path, recent)

    backup = tmp_path / "stdout.log.1"
    assert path.read_text(encoding="utf-8") == "3333\n"
    assert backup.read_text(encoding="utf-8") == "1111\n2222\n"
    assert path.stat().st_size <= 10
    assert backup.stat().st_size <= 10
    assert list(recent) == ["1111", "2222", "3333"]

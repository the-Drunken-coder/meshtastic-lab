from __future__ import annotations

import pytest

import scripts.acceptance as acceptance
from scripts.acceptance import AcceptanceFailure, listeners_closed


def test_listener_probe_treats_a_silent_successful_connection_as_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SilentConnection:
        def __enter__(self) -> SilentConnection:
            return self

        def __exit__(self, *_args: object) -> None:
            return

    def connect(_address: tuple[str, int], *, timeout: float) -> SilentConnection:
        assert timeout == 0.5
        return SilentConnection()

    monkeypatch.setattr(acceptance.socket, "create_connection", connect)

    with pytest.raises(AcceptanceFailure, match="45001"):
        listeners_closed([45001])

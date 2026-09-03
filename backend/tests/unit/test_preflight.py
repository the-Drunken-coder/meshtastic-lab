from __future__ import annotations

import pytest

from scripts import preflight


def test_port_preflight_identifies_only_unavailable_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_options: list[tuple[int, int, int]] = []

    class FakeListener:
        def setsockopt(self, level: int, option: int, value: int) -> None:
            socket_options.append((level, option, value))

        def bind(self, address: tuple[str, int]) -> None:
            if address[1] == 45001:
                raise OSError("already in use")

        def close(self) -> None:
            return

    monkeypatch.setattr(preflight.socket, "socket", lambda *_args: FakeListener())

    assert preflight.unavailable_ports("127.0.0.1", [8080, 45001]) == [45001]
    assert socket_options == [
        (preflight.socket.SOL_SOCKET, preflight.socket.SO_REUSEADDR, 1),
        (preflight.socket.SOL_SOCKET, preflight.socket.SO_REUSEADDR, 1),
    ]

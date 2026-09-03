from __future__ import annotations

import pytest

from scripts import preflight


def test_port_preflight_identifies_only_unavailable_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeListener:
        def bind(self, address: tuple[str, int]) -> None:
            if address[1] == 45001:
                raise OSError("already in use")

        def close(self) -> None:
            return

    monkeypatch.setattr(preflight.socket, "socket", lambda *_args: FakeListener())

    assert preflight.unavailable_ports("127.0.0.1", [8080, 45001]) == [45001]

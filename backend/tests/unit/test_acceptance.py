from __future__ import annotations

import pytest

import scripts.acceptance as acceptance
from scripts.acceptance import AcceptanceFailure, listeners_closed


def test_listener_probe_treats_a_silent_successful_connection_as_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SilentConnection:
        def settimeout(self, timeout: float) -> None:
            assert timeout == 0.5

        def sendall(self, data: bytes) -> None:
            assert data == b"\x94\xc3\x00\x00"

        def recv(self, _size: int) -> bytes:
            raise TimeoutError

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


@pytest.mark.parametrize("result", [b"", ConnectionResetError()])
def test_listener_probe_accepts_confirmed_proxy_side_close(
    monkeypatch: pytest.MonkeyPatch,
    result: bytes | ConnectionResetError,
) -> None:
    class ClosedConnection:
        def settimeout(self, _timeout: float) -> None:
            return

        def sendall(self, _data: bytes) -> None:
            return

        def recv(self, _size: int) -> bytes:
            if isinstance(result, ConnectionResetError):
                raise result
            return result

        def __enter__(self) -> ClosedConnection:
            return self

        def __exit__(self, *_args: object) -> None:
            return

    monkeypatch.setattr(
        acceptance.socket,
        "create_connection",
        lambda _address, *, timeout: ClosedConnection(),
    )

    listeners_closed([45001])

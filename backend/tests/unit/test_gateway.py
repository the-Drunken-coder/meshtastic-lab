from __future__ import annotations

import asyncio
import socket

import pytest

from backend.app.gateway import GatewayState, NodeGateway


class BlockingReader:
    def __init__(self) -> None:
        self.closed = asyncio.Event()

    async def read(self, _: int) -> bytes:
        await self.closed.wait()
        return b""


class FakeWriter:
    def __init__(self, peer: str) -> None:
        self.peer = peer
        self.closed = False

    def get_extra_info(self, name: str) -> str | None:
        return self.peer if name == "peername" else None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return

    def write(self, _: bytes) -> None:
        return

    async def drain(self) -> None:
        return


class OrderedServer:
    def __init__(self, writer: FakeWriter) -> None:
        self.writer = writer
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        assert self.writer.closed


class HangingWriter(FakeWriter):
    async def wait_closed(self) -> None:
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_public_port_is_reserved_before_downstream_start_and_released_on_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReservedSocket:
        def __init__(self) -> None:
            self.bound: tuple[str, int] | None = None
            self.closed = False

        def setsockopt(self, *_args: object) -> None:
            return

        def setblocking(self, _blocking: bool) -> None:
            return

        def bind(self, address: tuple[str, int]) -> None:
            self.bound = address

        def getsockname(self) -> tuple[str, int]:
            return "127.0.0.1", 45123

        def close(self) -> None:
            self.closed = True

    reserved = ReservedSocket()
    monkeypatch.setattr(socket, "socket", lambda *_args: reserved)
    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=1,
        public_host="127.0.0.1",
        public_port=0,
    )

    gateway.reserve_public_listener()
    assert reserved.bound == ("127.0.0.1", 0)
    assert gateway.public_port == 45123
    await gateway.stop()
    assert reserved.closed


@pytest.mark.asyncio
async def test_cancelled_start_closes_reserved_transports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=1,
        public_host="127.0.0.1",
        public_port=45001,
    )
    start_entered = asyncio.Event()
    cleanup_called = asyncio.Event()

    async def blocked_downstream() -> None:
        start_entered.set()
        await asyncio.Event().wait()

    async def cleanup() -> None:
        cleanup_called.set()

    monkeypatch.setattr(gateway, "reserve_public_listener", lambda: None)
    monkeypatch.setattr(gateway, "_establish_ready_downstream", blocked_downstream)
    monkeypatch.setattr(gateway, "_close_transports", cleanup)

    start_task = asyncio.create_task(gateway.start())
    await start_entered.wait()
    start_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start_task

    assert cleanup_called.is_set()
    assert gateway.state == GatewayState.FAILED


@pytest.mark.asyncio
async def test_internal_client_is_admitted_while_public_admission_is_disabled() -> None:
    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=1,
        public_host="127.0.0.1",
        public_port=0,
        public_clients_enabled=False,
    )
    gateway.state = GatewayState.RUNNING
    public_writer = FakeWriter("public-peer")
    public_reader = BlockingReader()
    internal_writer = FakeWriter("internal-peer")
    internal_reader = BlockingReader()
    try:
        await gateway._handle_external_client(public_reader, public_writer)  # type: ignore[arg-type]
        assert public_writer.closed
        assert not gateway.external_connected

        internal_task = asyncio.create_task(
            gateway._handle_internal_client(internal_reader, internal_writer)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0)
        assert gateway._client_writer is not None
        assert not gateway.external_connected
        assert not gateway.client_disconnected.is_set()

        second_writer = FakeWriter("second-public-peer")
        await gateway._handle_external_client(  # type: ignore[arg-type]
            BlockingReader(), second_writer
        )
        assert second_writer.closed
        assert gateway.rejected_clients == 2

        internal_reader.closed.set()
        await internal_task
        assert internal_writer.closed
        await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=1)
        assert not gateway.external_connected
    finally:
        internal_reader.closed.set()
        await asyncio.gather(*gateway._tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_enable_public_clients_admits_public_client_after_internal_setup() -> None:
    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=1,
        public_host="127.0.0.1",
        public_port=0,
        public_clients_enabled=False,
    )
    gateway.state = GatewayState.RUNNING
    public_writer = FakeWriter("public-peer")
    public_reader = BlockingReader()
    try:
        await gateway.enable_public_clients()
        public_task = asyncio.create_task(
            gateway._handle_external_client(public_reader, public_writer)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0)
        assert gateway.external_connected
        assert not gateway.client_disconnected.is_set()
        public_reader.closed.set()
        await public_task
        assert public_writer.closed
        await asyncio.wait_for(gateway.client_disconnected.wait(), timeout=1)
    finally:
        public_reader.closed.set()
        await asyncio.gather(*gateway._tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_close_transports_closes_clients_first_and_bounds_waits() -> None:
    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=1,
        public_host="127.0.0.1",
        public_port=0,
        shutdown_timeout=0.01,
    )
    client = HangingWriter("public-peer")
    server = OrderedServer(client)
    gateway._client_writer = client  # type: ignore[assignment]
    gateway._server = server  # type: ignore[assignment]

    await asyncio.wait_for(gateway._close_transports(), timeout=0.2)

    assert client.closed
    assert server.closed
    assert gateway._client_writer is None
    assert gateway._server is None


@pytest.mark.asyncio
async def test_readiness_retry_clears_stale_failure_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = NodeGateway(
        node_id="node-1",
        downstream_host="127.0.0.1",
        downstream_port=1,
        public_host="127.0.0.1",
        public_port=0,
        startup_timeout=1,
    )
    gateway.state = GatewayState.CONNECTING
    attempts = 0
    waiter_ready = asyncio.Event()

    class SignalingWaiters(dict[int, asyncio.Future[None]]):
        def __setitem__(self, key: int, value: asyncio.Future[None]) -> None:
            super().__setitem__(key, value)
            waiter_ready.set()

    gateway._config_waiters = SignalingWaiters()

    async def connect(_host: str, _port: int) -> tuple[asyncio.StreamReader, FakeWriter]:
        return asyncio.StreamReader(), FakeWriter("daemon")

    async def reader_loop() -> None:
        nonlocal attempts
        attempts += 1
        await waiter_ready.wait()
        waiter_ready.clear()
        waiter = next(iter(gateway._config_waiters.values()))
        if attempts == 1:
            await gateway._fail("daemon restarted")
        else:
            waiter.set_result(None)

    monkeypatch.setattr(asyncio, "open_connection", connect)
    monkeypatch.setattr(gateway, "_downstream_reader_loop", reader_loop)

    await gateway._establish_ready_downstream()

    assert attempts == 2
    assert gateway.state == GatewayState.CONNECTING
    assert not gateway.failed.is_set()
    await gateway.stop()

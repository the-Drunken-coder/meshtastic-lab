from __future__ import annotations

import asyncio

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

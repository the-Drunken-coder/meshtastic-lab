"""Async gateway that owns a node's only downstream Client API connection."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

from meshtastic.protobuf import mesh_pb2, portnums_pb2
from pydantic import BaseModel, ConfigDict

from .framing import Frame, FrameParser, encode_frame

LOGGER = logging.getLogger(__name__)


class GatewayError(RuntimeError):
    """Gateway startup, protocol, or shutdown failure."""


class GatewayState(StrEnum):
    STOPPED = "STOPPED"
    CONNECTING = "CONNECTING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    STOPPING = "STOPPING"


class GatewayEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    node_id: str
    kind: str
    detail: str
    frame: Frame | None = None


GatewayEventHandler = Callable[[GatewayEvent], Coroutine[object, object, None]]
FromRadioHandler = Callable[[str, mesh_pb2.FromRadio], Coroutine[object, object, None]]
QueueItem = TypeVar("QueueItem")


@dataclass(frozen=True, slots=True)
class _DownstreamWrite:
    raw: bytes
    source: str


class NodeGateway:
    """Multiplex controller writes and one external client onto one daemon stream.

    One task is the sole owner of downstream writes. The daemon reader forwards
    ordinary frames without re-encoding them and diverts SIMULATOR_APP frames to
    the RF controller.
    """

    def __init__(
        self,
        *,
        node_id: str,
        downstream_host: str,
        downstream_port: int,
        public_host: str,
        public_port: int,
        event_handler: GatewayEventHandler | None = None,
        from_radio_handler: FromRadioHandler | None = None,
        queue_size: int = 256,
        startup_timeout: float = 20.0,
        shutdown_timeout: float = 5.0,
        public_clients_enabled: bool = True,
    ) -> None:
        self.node_id = node_id
        self.downstream_host = downstream_host
        self.downstream_port = downstream_port
        self.public_host = public_host
        self.public_port = public_port
        self.public_clients_enabled = public_clients_enabled
        self.control_host = "127.0.0.1"
        self.control_port = 0
        self.event_handler = event_handler
        self.from_radio_handler = from_radio_handler
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout

        self.state = GatewayState.STOPPED
        self.external_connected = False
        self.failed = asyncio.Event()
        self.client_disconnected = asyncio.Event()
        self.client_disconnected.set()
        self.rejected_clients = 0
        self.dropped_external_frames = 0

        self.rf_frames: asyncio.Queue[mesh_pb2.MeshPacket] = asyncio.Queue(maxsize=queue_size)
        self._downstream_writes: asyncio.Queue[_DownstreamWrite] = asyncio.Queue(maxsize=queue_size)
        self._external_writes: asyncio.Queue[bytes] = asyncio.Queue(maxsize=queue_size)
        self._downstream_reader: asyncio.StreamReader | None = None
        self._downstream_writer: asyncio.StreamWriter | None = None
        self._client_writer: asyncio.StreamWriter | None = None
        self._server: asyncio.Server | None = None
        self._control_server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._config_waiters: dict[int, asyncio.Future[None]] = {}
        self._stop_lock = asyncio.Lock()

    @property
    def internal_port(self) -> int:
        """Return the ephemeral loopback port reserved for internal clients."""

        return self.control_port

    async def start(self) -> None:
        if self.state == GatewayState.RUNNING:
            return
        if self.state not in {GatewayState.STOPPED, GatewayState.FAILED}:
            raise GatewayError(f"cannot start gateway from {self.state}")

        self.state = GatewayState.CONNECTING
        self.failed.clear()
        try:
            await self._establish_ready_downstream()
            self._control_server = await asyncio.start_server(
                self._handle_internal_client,
                host=self.control_host,
                port=0,
                limit=4096,
            )
            control_socket = self._control_server.sockets[0]
            self.control_port = int(control_socket.getsockname()[1])
            self._server = await asyncio.start_server(
                self._handle_external_client,
                host=self.public_host,
                port=self.public_port,
                limit=4096,
            )
            self.state = GatewayState.RUNNING
            await self._emit("gateway.started", f"public port {self.public_port}")
        except Exception as exc:
            self.state = GatewayState.FAILED
            await self._close_transports()
            raise GatewayError(f"gateway {self.node_id} failed to start: {exc}") from exc

    async def stop(self) -> None:
        async with self._stop_lock:
            if self.state == GatewayState.STOPPED:
                return
            self.state = GatewayState.STOPPING
            await self._close_transports()

            tasks = tuple(self._tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                try:
                    async with asyncio.timeout(self.shutdown_timeout):
                        await asyncio.gather(*tasks, return_exceptions=True)
                except TimeoutError:
                    LOGGER.error("gateway task shutdown timed out", extra={"node_id": self.node_id})
            self._tasks.clear()
            self.external_connected = False
            self.state = GatewayState.STOPPED
            await self._emit("gateway.stopped", "gateway stopped")

    async def send_to_radio(self, message: mesh_pb2.ToRadio, *, source: str = "controller") -> None:
        if self.state != GatewayState.RUNNING:
            raise GatewayError(f"gateway is {self.state}")
        raw = encode_frame(message.SerializeToString())
        try:
            self._downstream_writes.put_nowait(_DownstreamWrite(raw=raw, source=source))
        except asyncio.QueueFull as exc:
            raise GatewayError(f"downstream queue full for {self.node_id}") from exc

    async def request_config(self, nonce: int, *, deadline_seconds: float = 20.0) -> None:
        loop = asyncio.get_running_loop()
        waiter = loop.create_future()
        self._config_waiters[nonce] = waiter
        request = mesh_pb2.ToRadio(want_config_id=nonce)
        try:
            await self.send_to_radio(request, source="controller.config")
            await asyncio.wait_for(waiter, timeout=deadline_seconds)
        finally:
            self._config_waiters.pop(nonce, None)

    async def inject_simulated_packet(self, packet: mesh_pb2.MeshPacket) -> None:
        request = mesh_pb2.ToRadio()
        request.packet.CopyFrom(packet)
        await self.send_to_radio(request, source="controller.rf")

    async def _establish_ready_downstream(self) -> None:
        last_error: OSError | None = None
        async with asyncio.timeout(self.startup_timeout):
            while True:
                try:
                    self._downstream_reader, self._downstream_writer = await asyncio.open_connection(
                        self.downstream_host, self.downstream_port
                    )
                except OSError as exc:
                    last_error = exc
                    await asyncio.sleep(0.05)
                    continue

                reader_task = self._spawn(
                    self._downstream_reader_loop(), name=f"{self.node_id}-daemon-reader"
                )
                writer_task = self._spawn(
                    self._downstream_writer_loop(), name=f"{self.node_id}-daemon-writer"
                )
                nonce = 0x4D4C0001
                waiter = asyncio.get_running_loop().create_future()
                self._config_waiters[nonce] = waiter
                request = mesh_pb2.ToRadio(want_config_id=nonce)
                await self._downstream_writes.put(
                    _DownstreamWrite(
                        raw=encode_frame(request.SerializeToString()), source="gateway.readiness"
                    )
                )
                try:
                    await waiter
                except GatewayError:
                    self.state = GatewayState.CONNECTING
                    await self._discard_downstream_attempt(reader_task, writer_task)
                    last_error = ConnectionResetError("daemon closed during readiness handshake")
                    await asyncio.sleep(0.05)
                    continue
                finally:
                    self._config_waiters.pop(nonce, None)
                await self._emit("gateway.downstream_ready", f"config nonce {nonce}")
                return

        raise GatewayError(f"downstream connection failed: {last_error}")

    async def _discard_downstream_attempt(
        self, reader_task: asyncio.Task[None], writer_task: asyncio.Task[None]
    ) -> None:
        for task in (reader_task, writer_task):
            task.cancel()
        await asyncio.gather(reader_task, writer_task, return_exceptions=True)
        if self._downstream_writer is not None:
            self._downstream_writer.close()
            with contextlib.suppress(Exception):
                await self._downstream_writer.wait_closed()
        self._downstream_reader = None
        self._downstream_writer = None
        self._clear_queue(self._downstream_writes)

    def _spawn(self, coroutine: Coroutine[object, object, None], *, name: str) -> asyncio.Task[None]:
        task: asyncio.Task[None] = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def enable_public_clients(self) -> None:
        """Admit public clients after internal startup/configuration is complete."""

        if self.state in {GatewayState.STOPPED, GatewayState.STOPPING}:
            raise GatewayError(f"cannot enable public clients while gateway is {self.state}")
        self.public_clients_enabled = True
        await self._emit("gateway.public_clients_enabled", "public client admission enabled")

    async def _handle_external_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await self._handle_client(reader, writer, public=True)

    async def _handle_internal_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await self._handle_client(reader, writer, public=False)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        public: bool,
    ) -> None:
        peer = writer.get_extra_info("peername")
        if public and not self.public_clients_enabled:
            self.rejected_clients += 1
            await self._emit("gateway.client_rejected", f"public client admission disabled: {peer}")
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return
        if self._client_writer is not None:
            self.rejected_clients += 1
            await self._emit("gateway.client_rejected", f"second client rejected: {peer}")
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return

        self.external_connected = public
        self.client_disconnected.clear()
        self._client_writer = writer
        await self._emit(
            "gateway.client_connected", f"{'public' if public else 'internal'}: {peer}"
        )
        reader_task = asyncio.create_task(
            self._external_reader_loop(reader), name=f"{self.node_id}-client-reader"
        )
        writer_task = asyncio.create_task(
            self._external_writer_loop(writer), name=f"{self.node_id}-client-writer"
        )
        self._tasks.update({reader_task, writer_task})
        try:
            done, pending = await asyncio.wait(
                {reader_task, writer_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
        finally:
            self._tasks.discard(reader_task)
            self._tasks.discard(writer_task)
            if self._client_writer is writer:
                self._client_writer = None
            self.external_connected = False
            self.client_disconnected.set()
            self._clear_queue(self._external_writes)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            await self._emit("gateway.client_disconnected", str(peer))

    async def _external_reader_loop(self, reader: asyncio.StreamReader) -> None:
        parser = FrameParser()
        while data := await reader.read(4096):
            for frame in parser.feed(data):
                await self._downstream_writes.put(_DownstreamWrite(raw=frame.raw, source="external"))

    async def _external_writer_loop(self, writer: asyncio.StreamWriter) -> None:
        while True:
            raw = await self._external_writes.get()
            writer.write(raw)
            await writer.drain()

    async def _downstream_reader_loop(self) -> None:
        if self._downstream_reader is None:
            raise GatewayError("downstream reader was not initialized")

        parser = FrameParser()
        try:
            while data := await self._downstream_reader.read(4096):
                for frame in parser.feed(data):
                    await self._handle_downstream_frame(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._fail(f"downstream read failed: {exc}")
            return
        if self.state not in {GatewayState.STOPPING, GatewayState.STOPPED}:
            await self._fail("downstream daemon disconnected")

    async def _downstream_writer_loop(self) -> None:
        if self._downstream_writer is None:
            raise GatewayError("downstream writer was not initialized")
        try:
            while True:
                write = await self._downstream_writes.get()
                self._downstream_writer.write(write.raw)
                await self._downstream_writer.drain()
                await self._emit("gateway.downstream_write", write.source)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._fail(f"downstream write failed: {exc}")

    async def _handle_downstream_frame(self, frame: Frame) -> None:
        message = mesh_pb2.FromRadio()
        try:
            message.ParseFromString(frame.payload)
        except Exception as exc:
            await self._emit("gateway.invalid_from_radio", str(exc), frame=frame)
            return

        if self.from_radio_handler is not None:
            await self.from_radio_handler(self.node_id, message)

        if message.WhichOneof("payload_variant") == "config_complete_id":
            waiter = self._config_waiters.get(message.config_complete_id)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)

        is_simulated_rf = (
            message.WhichOneof("payload_variant") == "packet"
            and message.packet.WhichOneof("payload_variant") == "decoded"
            and message.packet.decoded.portnum == portnums_pb2.SIMULATOR_APP
        )
        if is_simulated_rf:
            packet = mesh_pb2.MeshPacket()
            packet.CopyFrom(message.packet)
            try:
                self.rf_frames.put_nowait(packet)
            except asyncio.QueueFull:
                await self._emit("gateway.rf_queue_full", "RF frame dropped from controller queue")
                return
            await self._emit("gateway.rf_transmit", str(packet.id), frame=frame)
            return

        if self._client_writer is not None:
            try:
                self._external_writes.put_nowait(frame.raw)
            except asyncio.QueueFull:
                self.dropped_external_frames += 1
                await self._emit("gateway.external_backpressure", "external frame queue full")

    async def _fail(self, detail: str) -> None:
        if self.state in {GatewayState.STOPPING, GatewayState.STOPPED, GatewayState.FAILED}:
            return
        self.state = GatewayState.FAILED
        self.failed.set()
        await self._emit("gateway.failed", detail)
        for waiter in self._config_waiters.values():
            if not waiter.done():
                waiter.set_exception(GatewayError(detail))
        if self._client_writer is not None:
            self._client_writer.close()

    async def _emit(self, kind: str, detail: str, *, frame: Frame | None = None) -> None:
        LOGGER.info(detail, extra={"node_id": self.node_id, "event": kind})
        if self.event_handler is not None:
            await self.event_handler(
                GatewayEvent(node_id=self.node_id, kind=kind, detail=detail, frame=frame)
            )

    async def _close_transports(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._control_server is not None:
            self._control_server.close()
            await self._control_server.wait_closed()
            self._control_server = None

        for writer in (self._client_writer, self._downstream_writer):
            if writer is not None:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
        self._client_writer = None
        self._downstream_writer = None
        self._downstream_reader = None

        for waiter in self._config_waiters.values():
            if not waiter.done():
                waiter.set_exception(GatewayError(f"gateway {self.node_id} stopped"))
        self._config_waiters.clear()

    @staticmethod
    def _clear_queue(queue: asyncio.Queue[QueueItem]) -> None:
        while not queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()

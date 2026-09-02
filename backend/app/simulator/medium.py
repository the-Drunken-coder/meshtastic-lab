"""Deterministic directed-link RF forwarding."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping

from meshtastic.protobuf import mesh_pb2

from backend.app.gateway import NodeGateway
from backend.app.metrics import EventBroker, EventType, PacketEvent, airtime_ms, mesh_packet_payload_length
from backend.app.models import DirectedLink, Scenario

TransmissionHandler = Callable[[str, mesh_pb2.MeshPacket, int], None]
DropHandler = Callable[[str, mesh_pb2.MeshPacket, str], None]
FailureHandler = Callable[[str, Exception], Awaitable[None]]
LOGGER = logging.getLogger(__name__)


class DirectedMedium:
    """Forward firmware-created RF frames across an atomic directed graph."""

    def __init__(
        self,
        *,
        scenario: Scenario,
        gateways: Mapping[str, NodeGateway],
        event_broker: EventBroker,
        hardware_ids: Mapping[str, int],
        transmission_handler: TransmissionHandler | None = None,
        drop_handler: DropHandler | None = None,
        failure_handler: FailureHandler | None = None,
    ) -> None:
        self._scenario = scenario
        self._gateways = gateways
        self._event_broker = event_broker
        self._hardware_ids = hardware_ids
        self._transmission_handler = transmission_handler
        self._drop_handler = drop_handler
        self._failure_handler = failure_handler
        self._links = scenario.link_map()
        self._link_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(
                self._supervised_node_loop(node_id, gateway), name=f"medium-{node_id}"
            )
            for node_id, gateway in self._gateways.items()
        ]

    async def stop(self) -> None:
        tasks, self._tasks = self._tasks, []
        current_task = asyncio.current_task()
        for task in tasks:
            if task is not current_task:
                task.cancel()
        waiters = [task for task in tasks if task is not current_task]
        if waiters:
            await asyncio.gather(*waiters, return_exceptions=True)

    async def update_link(self, link: DirectedLink) -> None:
        key = (link.from_node, link.to_node)
        async with self._link_lock:
            if key not in self._links:
                raise KeyError(f"unknown directed link: {link.from_node} -> {link.to_node}")
            updated = dict(self._links)
            updated[key] = link
            self._links = updated
        self._event_broker.publish(
            PacketEvent(
                monotonicSeconds=time.monotonic(),
                eventType=EventType.LINK_UPDATED,
                transmitter=link.from_node,
                receiver=link.to_node,
                result="enabled" if link.enabled else "disabled",
                rssiDbm=link.rssi_dbm,
                snrDb=link.snr_db,
            )
        )
        LOGGER.info(
            "directed link updated",
            extra={
                "node_id": link.from_node,
                "receiver_node_id": link.to_node,
                "link_decision": "enabled" if link.enabled else "disabled",
            },
        )

    async def apply_links(self, links: list[DirectedLink]) -> None:
        replacement = {(link.from_node, link.to_node): link for link in links}
        async with self._link_lock:
            if replacement.keys() != self._links.keys():
                raise ValueError("runtime topology must contain the existing directed link set")
            self._links = replacement

    async def transmit(self, transmitter: str, packet: mesh_pb2.MeshPacket) -> None:
        monotonic_now = time.monotonic()
        async with self._link_lock:
            links = tuple(
                link for (source, _), link in self._links.items() if source == transmitter
            )

        enabled = [link for link in links if link.enabled]
        payload_length = mesh_packet_payload_length(packet)
        packet_airtime = airtime_ms(payload_length, self._scenario.rf.modem_preset)
        self._event_broker.publish(
            PacketEvent(
                monotonicSeconds=monotonic_now,
                eventType=EventType.RF_TRANSMIT,
                transmitter=transmitter,
                intendedDestination=self._destination_node(packet.to),
                receiverSet=[link.to_node for link in enabled],
                meshPacketId=packet.id,
                hopLimit=packet.hop_limit,
                hopStart=packet.hop_start,
                packetLength=payload_length,
                airtimeMs=packet_airtime,
                result="transmitted",
            )
        )
        if self._transmission_handler is not None:
            self._transmission_handler(transmitter, packet, packet_airtime)

        injections: list[asyncio.Task[None]] = []
        for link in links:
            if not link.enabled:
                LOGGER.info(
                    "RF frame dropped at directed link",
                    extra={
                        "node_id": transmitter,
                        "receiver_node_id": link.to_node,
                        "packet_id": packet.id,
                        "link_decision": "disabled",
                        "error_category": "link-disabled",
                    },
                )
                self._event_broker.publish(
                    PacketEvent(
                        monotonicSeconds=monotonic_now,
                        eventType=EventType.LINK_DISABLED,
                        transmitter=transmitter,
                        receiver=link.to_node,
                        meshPacketId=packet.id,
                        result="link-disabled",
                    )
                )
                if self._drop_handler is not None:
                    self._drop_handler(transmitter, packet, "link-disabled")
                continue
            received = mesh_pb2.MeshPacket()
            received.CopyFrom(packet)
            received.rx_rssi = link.rssi_dbm
            received.rx_snr = link.snr_db
            injections.append(
                asyncio.create_task(
                    self._inject(link, received, monotonic_now),
                    name=f"inject-{transmitter}-{link.to_node}-{packet.id}",
                )
            )
        if injections:
            await asyncio.gather(*injections)

    async def _node_loop(self, node_id: str, gateway: NodeGateway) -> None:
        while True:
            packet = await gateway.rf_frames.get()
            await self.transmit(node_id, packet)

    async def _supervised_node_loop(self, node_id: str, gateway: NodeGateway) -> None:
        """Keep permanent RF workers observable when a transmit fails."""

        try:
            await self._node_loop(node_id, gateway)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._report_failure(node_id, exc)
        else:
            await self._report_failure(node_id, RuntimeError("medium worker exited unexpectedly"))

    async def _report_failure(self, node_id: str, exc: Exception) -> None:
        if self._failure_handler is None:
            LOGGER.error(
                "directed medium worker failed",
                extra={"node_id": node_id, "error_category": "medium-worker-failed"},
                exc_info=exc,
            )
            return
        try:
            await self._failure_handler(node_id, exc)
        except Exception:
            LOGGER.exception(
                "directed medium failure handler failed",
                extra={"node_id": node_id, "error_category": "medium-failure-handler"},
            )

    async def _inject(
        self, link: DirectedLink, packet: mesh_pb2.MeshPacket, monotonic_now: float
    ) -> None:
        await self._gateways[link.to_node].inject_simulated_packet(packet)
        self._event_broker.publish(
            PacketEvent(
                monotonicSeconds=monotonic_now,
                eventType=EventType.RX_INJECTED,
                transmitter=link.from_node,
                receiver=link.to_node,
                meshPacketId=packet.id,
                hopLimit=packet.hop_limit,
                hopStart=packet.hop_start,
                rssiDbm=link.rssi_dbm,
                snrDb=link.snr_db,
                result="injected-to-firmware",
            )
        )

    def _destination_node(self, node_number: int) -> str | None:
        if node_number == 0xFFFFFFFF:
            return "broadcast"
        return next(
            (node_id for node_id, hardware_id in self._hardware_ids.items() if hardware_id == node_number),
            f"!{node_number:08x}" if node_number else None,
        )

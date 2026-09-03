"""LoRa airtime calculation aligned with native SimRadio."""

from __future__ import annotations

import math
from dataclasses import dataclass

from meshtastic.protobuf import mesh_pb2, portnums_pb2

MESH_PACKET_HEADER_BYTES = 16
MAX_RETRANSMISSION_CONTENTION_SLOTS = (1 << 8) + 2 * 8 + (1 << 5)
RETRANSMISSION_PROCESSING_TIME_MS = 4500
SLOT_FIXED_OVERHEAD_MS = 7.6


@dataclass(frozen=True, slots=True)
class ModemParameters:
    bandwidth_khz: float
    spreading_factor: int
    coding_rate: int


MODEM_PARAMETERS: dict[str, ModemParameters] = {
    "SHORT_TURBO": ModemParameters(500, 7, 5),
    "SHORT_FAST": ModemParameters(250, 7, 5),
    "SHORT_SLOW": ModemParameters(250, 8, 5),
    "MEDIUM_FAST": ModemParameters(250, 9, 5),
    "MEDIUM_SLOW": ModemParameters(250, 10, 5),
    "LONG_TURBO": ModemParameters(500, 11, 8),
    "LONG_MODERATE": ModemParameters(125, 11, 8),
    "LONG_SLOW": ModemParameters(125, 12, 8),
    "LONG_FAST": ModemParameters(250, 11, 5),
}


def airtime_ms(payload_length: int, modem_preset: str, *, preamble_symbols: int = 16) -> int:
    """Return native SimRadio's integer millisecond packet airtime."""

    if payload_length < 0:
        raise ValueError("payload length cannot be negative")
    try:
        params = MODEM_PARAMETERS[modem_preset]
    except KeyError as exc:
        raise ValueError(f"unsupported modem preset: {modem_preset}") from exc

    bandwidth_hz = params.bandwidth_khz * 1000
    symbol_time = (1 << params.spreading_factor) / bandwidth_hz
    low_data_rate_optimization = symbol_time > 0.016
    preamble_time = (preamble_symbols + 4.25) * symbol_time
    numerator = 8 * payload_length - 4 * params.spreading_factor + 28 + 16
    denominator = 4 * (params.spreading_factor - 2 * int(low_data_rate_optimization))
    payload_symbols = 8 + max(math.ceil(numerator / denominator * params.coding_rate), 0)
    return int((preamble_time + payload_symbols * symbol_time) * 1000)


def maximum_retransmission_delay_ms(payload_length: int, modem_preset: str) -> int:
    """Bound the pinned firmware's delay between transmission attempts."""

    try:
        params = MODEM_PARAMETERS[modem_preset]
    except KeyError as exc:
        raise ValueError(f"unsupported modem preset: {modem_preset}") from exc
    symbol_time_ms = (1 << params.spreading_factor) / params.bandwidth_khz
    slot_time_ms = 2.5 * symbol_time_ms + SLOT_FIXED_OVERHEAD_MS
    packet_airtime_ms = airtime_ms(payload_length, modem_preset)
    return math.ceil(
        2 * packet_airtime_ms
        + MAX_RETRANSMISSION_CONTENTION_SLOTS * slot_time_ms
        + RETRANSMISSION_PROCESSING_TIME_MS
    )


def mesh_packet_payload_length(packet: mesh_pb2.MeshPacket) -> int:
    """Recover the length SimRadio used before wrapping a transmitted packet."""

    if packet.WhichOneof("payload_variant") == "encrypted":
        return len(packet.encrypted) + MESH_PACKET_HEADER_BYTES

    if packet.WhichOneof("payload_variant") != "decoded":
        return MESH_PACKET_HEADER_BYTES

    data = mesh_pb2.Data()
    data.CopyFrom(packet.decoded)
    if data.portnum == portnums_pb2.SIMULATOR_APP:
        compressed = mesh_pb2.Compressed()
        compressed.ParseFromString(data.payload)
        if compressed.portnum == portnums_pb2.UNKNOWN_APP:
            return len(compressed.data) + MESH_PACKET_HEADER_BYTES
        data.portnum = compressed.portnum
        data.payload = compressed.data
    return len(data.SerializeToString()) + MESH_PACKET_HEADER_BYTES


def mesh_packet_port_number(packet: mesh_pb2.MeshPacket) -> int | None:
    """Return the application port carried by a decoded RF packet."""

    if packet.WhichOneof("payload_variant") != "decoded":
        return None
    if packet.decoded.portnum != portnums_pb2.SIMULATOR_APP:
        return int(packet.decoded.portnum)
    compressed = mesh_pb2.Compressed()
    try:
        compressed.ParseFromString(packet.decoded.payload)
    except Exception:
        return int(portnums_pb2.SIMULATOR_APP)
    return int(compressed.portnum)

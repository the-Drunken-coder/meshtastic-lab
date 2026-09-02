"""Meshtastic TCP and serial stream framing.

The parser keeps arbitrary read boundaries out of the rest of the gateway. It
also mirrors firmware resynchronization behavior after noise or a bad length.
"""

from __future__ import annotations

from dataclasses import dataclass

MAGIC = b"\x94\xc3"
HEADER_SIZE = 4
MAX_PROTOBUF_SIZE = 512


@dataclass(frozen=True, slots=True)
class Frame:
    """One complete framed protobuf, retaining its exact wire bytes."""

    raw: bytes
    payload: bytes


class FrameParser:
    """Incrementally parse framed protobufs and recover after invalid input."""

    def __init__(self, *, maximum_payload_size: int = MAX_PROTOBUF_SIZE) -> None:
        self._buffer = bytearray()
        self.maximum_payload_size = maximum_payload_size
        self.discarded_bytes = 0
        self.oversized_frames = 0

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, data: bytes) -> list[Frame]:
        if data:
            self._buffer.extend(data)

        frames: list[Frame] = []
        while self._buffer:
            start = self._buffer.find(MAGIC)
            if start < 0:
                keep = 1 if self._buffer[-1] == MAGIC[0] else 0
                discarded = len(self._buffer) - keep
                self.discarded_bytes += discarded
                if keep:
                    self._buffer[:] = self._buffer[-1:]
                else:
                    self._buffer.clear()
                break

            if start:
                del self._buffer[:start]
                self.discarded_bytes += start

            if len(self._buffer) < HEADER_SIZE:
                break

            payload_size = int.from_bytes(self._buffer[2:4], byteorder="big")
            if payload_size > self.maximum_payload_size:
                self.oversized_frames += 1
                self.discarded_bytes += 1
                del self._buffer[0]
                continue

            frame_size = HEADER_SIZE + payload_size
            if len(self._buffer) < frame_size:
                break

            raw = bytes(self._buffer[:frame_size])
            del self._buffer[:frame_size]
            frames.append(Frame(raw=raw, payload=raw[HEADER_SIZE:]))

        return frames


def encode_frame(payload: bytes) -> bytes:
    """Frame one ToRadio or FromRadio protobuf."""

    if len(payload) > MAX_PROTOBUF_SIZE:
        raise ValueError(f"protobuf payload is {len(payload)} bytes, maximum is {MAX_PROTOBUF_SIZE}")
    return MAGIC + len(payload).to_bytes(2, byteorder="big") + payload

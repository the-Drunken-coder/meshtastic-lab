# Client connections

Every virtual radio exposes the official Meshtastic TCP streaming protocol at its public endpoint. There is no simulator SDK or extra envelope.

## Framing and handshake

Both directions use magic bytes `94 c3`, a two-byte big-endian protobuf length, and at most 512 protobuf bytes. An external official client sends its wake bytes and `want_config_id`. The gateway forwards that exact request to the already-owned downstream stream. The firmware emits local information, metadata, node records, channel and configuration records, then a matching `config_complete_id`. Those frames are forwarded byte for byte.

The parser buffers arbitrary TCP boundaries and resynchronizes after invalid bytes. It can extract partial headers and several frames from one read. A length above the protocol maximum is counted as invalid framing, then the parser discards one byte and searches for the next valid frame boundary.

## One client per node

V1 accepts one external connection per gateway. A second connection is rejected before it can affect the active stream. This is a deliberate product limit, not a limitation across the network: node 1 and node 2 can each have a client at the same time.

Startup configuration and NodeInfo requests use a separate ephemeral listener bound to `127.0.0.1` inside the application container. Public admission remains disabled through `STARTING` and `WARMING_UP`. A host application may keep retrying a published node port, but it cannot claim the internal configuration slot and is admitted only after the lifecycle reaches `RUNNING`.

When the client disconnects, the gateway retains its downstream daemon connection and RF-control path. A new external connection receives a fresh normal Client API configuration exchange. If the downstream daemon disconnects, the gateway enters `FAILED`; while the simulation is running, the lifecycle also fails and performs bounded cleanup.

## Shared downstream writer

External application requests, internal configuration requests, RF injections, and built-in traffic all use one serialized downstream write task. They cannot interleave bytes. The source of a controller write is logged for diagnostics, but no source marker appears on the Meshtastic wire.

The built-in traffic generator creates ordinary `ToRadio.packet` messages and enters this same writer. It does not call the RF medium. Incoming application packets and routing responses are observed from ordinary `FromRadio` messages before the preserved frame is forwarded to the external client.

`SIMULATOR_APP` is the one classified exception. It is the native firmware’s outgoing RF representation, so the gateway places it on the controller RF queue instead of presenting it as an application packet. The receiving gateway injects it with ordinary `ToRadio.packet`; the receiving firmware then exposes any decoded result normally.

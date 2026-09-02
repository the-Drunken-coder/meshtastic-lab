"""Configure one node with the official client and verify effective values."""

from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Event
from typing import Any

from meshtastic import tcp_interface
from meshtastic.protobuf import admin_pb2, channel_pb2, config_pb2, portnums_pb2

from backend.app.models import RFSettings, ScenarioChannel, ScenarioNode


class NodeConfigurationError(RuntimeError):
    """The daemon did not apply or report the requested node configuration."""


@dataclass(frozen=True, slots=True)
class NodeVerification:
    node_id: str
    node_number: int
    firmware_version: str
    owner_long_name: str
    owner_short_name: str
    role: str
    region: str
    modem_preset: str
    frequency_slot: int
    hop_limit: int
    channel_name: str


def configure_and_verify_node(
    *,
    hostname: str,
    port: int,
    node: ScenarioNode,
    rf: RFSettings,
    channel: ScenarioChannel,
    deadline_seconds: float = 45.0,
    reboot_after_apply: bool = False,
) -> NodeVerification:
    """Write requested values, reconnect, and compare the fresh config dump."""

    deadline = time.monotonic() + deadline_seconds
    interface = _connect(hostname, port, deadline)
    try:
        local = interface.localNode
        begin_edit = admin_pb2.AdminMessage(begin_edit_settings=True)
        _send_admin_and_wait(interface, begin_edit, deadline)

        owner = admin_pb2.AdminMessage()
        owner.set_owner.long_name = node.display_name
        owner.set_owner.short_name = _short_name(node)
        _send_admin_and_wait(interface, owner, deadline)

        lora = config_pb2.Config.LoRaConfig()
        lora.CopyFrom(local.localConfig.lora)
        lora.use_preset = True
        lora.region = config_pb2.Config.LoRaConfig.RegionCode.Value(rf.region)
        lora.modem_preset = config_pb2.Config.LoRaConfig.ModemPreset.Value(rf.modem_preset)
        lora.channel_num = rf.frequency_slot
        lora.hop_limit = rf.hop_limit
        lora.tx_enabled = True
        set_lora = admin_pb2.AdminMessage()
        set_lora.set_config.lora.CopyFrom(lora)
        _send_admin_and_wait(interface, set_lora, deadline)

        requested_role = config_pb2.Config.DeviceConfig.Role.Value(node.role.value)
        if local.localConfig.device.role != requested_role:
            device = config_pb2.Config.DeviceConfig()
            device.CopyFrom(local.localConfig.device)
            device.role = requested_role
            set_device = admin_pb2.AdminMessage()
            set_device.set_config.device.CopyFrom(device)
            _send_admin_and_wait(interface, set_device, deadline)

        primary = channel_pb2.Channel()
        primary.CopyFrom(local.channels[0])
        primary.role = channel_pb2.Channel.Role.PRIMARY
        primary.settings.name = channel.name
        primary.settings.psk = channel.key_bytes()
        set_channel = admin_pb2.AdminMessage()
        set_channel.set_channel.CopyFrom(primary)
        _send_admin_and_wait(interface, set_channel, deadline)

        # A settings transaction applies the complete scenario atomically and
        # suppresses the per-setting native reboot that would otherwise exit a
        # Portduino child before the simulator becomes ready.
        commit_edit = admin_pb2.AdminMessage(commit_edit_settings=True)
        _send_admin_and_wait(interface, commit_edit, deadline)
    finally:
        interface.close()

    verified = _connect(hostname, port, deadline)
    try:
        result = _verify(verified, node=node, rf=rf, channel=channel)
        if reboot_after_apply:
            # The official client deliberately treats local reboot as a
            # one-shot command. Waiting for a response races its automatic TCP
            # reconnect with the daemon's one-second restart deadline.
            verified.localNode.reboot(1)
        return result
    finally:
        verified.close()


def verify_node(
    *,
    hostname: str,
    port: int,
    node: ScenarioNode,
    rf: RFSettings,
    channel: ScenarioChannel,
    deadline_seconds: float = 45.0,
) -> NodeVerification:
    """Read configuration from a fresh official-client session and compare it."""

    deadline = time.monotonic() + deadline_seconds
    interface = _connect(hostname, port, deadline)
    try:
        return _verify(interface, node=node, rf=rf, channel=channel)
    finally:
        interface.close()


def request_node_info(
    *, hostname: str, port: int, deadline_seconds: float = 30.0
) -> int:
    """Ask peers for NodeInfo through a normal official-client firmware send."""

    deadline = time.monotonic() + deadline_seconds
    interface = _connect(hostname, port, deadline)
    try:
        packet = interface.sendData(
            b"",
            destinationId="^all",
            portNum=portnums_pb2.NODEINFO_APP,
            wantAck=False,
            wantResponse=True,
        )
        return int(packet.id)
    finally:
        interface.close()


def _connect(hostname: str, port: int, deadline: float) -> tcp_interface.TCPInterface:
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return tcp_interface.TCPInterface(
                hostname=hostname,
                portNumber=port,
                timeout=max(1, int(deadline - time.monotonic())),
            )
        except Exception as exc:
            last_error = exc
            time.sleep(0.1)
    raise NodeConfigurationError(f"official client could not complete config handshake: {last_error}")


def _send_admin_and_wait(
    interface: tcp_interface.TCPInterface, message: admin_pb2.AdminMessage, deadline: float
) -> None:
    completed = Event()
    response: list[dict[str, Any]] = []

    def on_response(packet: dict[str, Any]) -> None:
        response.append(packet)
        completed.set()

    interface.sendData(
        message,
        destinationId=interface.myInfo.my_node_num,
        portNum=portnums_pb2.ADMIN_APP,
        wantAck=True,
        wantResponse=True,
        onResponse=on_response,
        onResponseAckPermitted=True,
        pkiEncrypted=True,
    )
    remaining = deadline - time.monotonic()
    if remaining <= 0 or not completed.wait(remaining):
        raise NodeConfigurationError("timed out waiting for firmware admin response")
    decoded = response[-1].get("decoded", {})
    routing = decoded.get("routing") if isinstance(decoded, dict) else None
    if isinstance(routing, dict) and routing.get("errorReason") not in {None, "NONE"}:
        raise NodeConfigurationError(f"firmware rejected admin write: {routing['errorReason']}")


def _verify(
    interface: tcp_interface.TCPInterface,
    *,
    node: ScenarioNode,
    rf: RFSettings,
    channel: ScenarioChannel,
) -> NodeVerification:
    if interface.myInfo is None or interface.metadata is None:
        raise NodeConfigurationError(f"{node.id} did not return my_info and metadata")
    local = interface.localNode
    lora = local.localConfig.lora
    device = local.localConfig.device
    primary = local.channels[0]
    node_entry = interface.nodesByNum.get(interface.myInfo.my_node_num, {})
    user = node_entry.get("user", {})

    actual = {
        "owner_long_name": user.get("longName", ""),
        "owner_short_name": user.get("shortName", ""),
        "role": config_pb2.Config.DeviceConfig.Role.Name(device.role),
        "region": config_pb2.Config.LoRaConfig.RegionCode.Name(lora.region),
        "modem_preset": config_pb2.Config.LoRaConfig.ModemPreset.Name(lora.modem_preset),
        "frequency_slot": lora.channel_num,
        "hop_limit": lora.hop_limit,
        "channel_name": primary.settings.name,
        "channel_psk": bytes(primary.settings.psk),
    }
    expected = {
        "owner_long_name": node.display_name,
        "owner_short_name": _short_name(node),
        "role": node.role.value,
        "region": rf.region,
        "modem_preset": rf.modem_preset,
        "frequency_slot": rf.frequency_slot,
        "hop_limit": rf.hop_limit,
        "channel_name": channel.name,
        "channel_psk": channel.key_bytes(),
    }
    differences = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in expected
        if actual[key] != expected[key]
    }
    if differences:
        raise NodeConfigurationError(f"{node.id} configuration verification failed: {differences}")

    return NodeVerification(
        node_id=node.id,
        node_number=interface.myInfo.my_node_num,
        firmware_version=interface.metadata.firmware_version,
        owner_long_name=actual["owner_long_name"],
        owner_short_name=actual["owner_short_name"],
        role=actual["role"],
        region=actual["region"],
        modem_preset=actual["modem_preset"],
        frequency_slot=actual["frequency_slot"],
        hop_limit=actual["hop_limit"],
        channel_name=actual["channel_name"],
    )


def _short_name(node: ScenarioNode) -> str:
    suffix = node.id.removeprefix("node-")
    return f"N{suffix}"[:4]

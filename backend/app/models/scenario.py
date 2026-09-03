"""Versioned scenario schema and deterministic topology presets."""

from __future__ import annotations

import base64
import binascii
from collections import deque
from enum import StrEnum
from typing import Annotated, Literal

from meshtastic.protobuf import config_pb2
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PUBLIC_PORT_MIN = 45001
PUBLIC_PORT_MAX = 45010
SUPPORTED_MODEM_PRESETS = {
    "LONG_FAST",
    "LONG_SLOW",
    "MEDIUM_SLOW",
    "MEDIUM_FAST",
    "SHORT_SLOW",
    "SHORT_FAST",
    "LONG_MODERATE",
    "SHORT_TURBO",
    "LONG_TURBO",
}


class NodeRole(StrEnum):
    CLIENT = "CLIENT"
    CLIENT_MUTE = "CLIENT_MUTE"
    ROUTER = "ROUTER"
    REPEATER = "REPEATER"


class CollisionMode(StrEnum):
    NATIVE = "native"


class TopologyPreset(StrEnum):
    FULL_MESH = "full-mesh"
    LINE = "line"
    STAR = "star"
    ALL_ISOLATED = "all-isolated"


class RFSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    region: str = "US"
    modem_preset: str = Field(default="LONG_FAST", alias="modemPreset")
    frequency_slot: Annotated[int, Field(ge=0, le=255)] = Field(default=20, alias="frequencySlot")
    hop_limit: Annotated[int, Field(ge=1, le=7)] = Field(default=4, alias="hopLimit")
    collision_mode: CollisionMode = Field(default=CollisionMode.NATIVE, alias="collisionMode")

    @field_validator("region")
    @classmethod
    def validate_region(cls, value: str) -> str:
        if value == "UNSET":
            raise ValueError("Meshtastic region must be set")
        try:
            config_pb2.Config.LoRaConfig.RegionCode.Value(value)
        except ValueError as exc:
            raise ValueError(f"unsupported Meshtastic region: {value}") from exc
        return value

    @model_validator(mode="after")
    def validate_preset(self) -> RFSettings:
        if self.modem_preset not in SUPPORTED_MODEM_PRESETS:
            raise ValueError(f"unsupported modem preset: {self.modem_preset}")
        return self


class ScenarioChannel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=12)] = "Simulator"
    psk: str = "AQ=="

    @model_validator(mode="after")
    def validate_psk(self) -> ScenarioChannel:
        try:
            decoded = base64.b64decode(self.psk, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("psk must be valid base64") from exc
        if len(decoded) not in {1, 16, 32}:
            raise ValueError("psk must decode to 1, 16, or 32 bytes")
        return self

    def key_bytes(self) -> bytes:
        return base64.b64decode(self.psk, validate=True)


class ScenarioNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{0,31}$")]
    display_name: Annotated[str, Field(min_length=1, max_length=40)] = Field(alias="displayName")
    role: NodeRole = NodeRole.CLIENT
    api_port: Annotated[int, Field(ge=PUBLIC_PORT_MIN, le=PUBLIC_PORT_MAX)] = Field(alias="apiPort")


class DirectedLink(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    from_node: str = Field(alias="from")
    to_node: str = Field(alias="to")
    enabled: bool = True
    rssi_dbm: Annotated[int, Field(ge=-200, le=0)] = Field(default=-85, alias="rssiDbm")
    snr_db: Annotated[float, Field(ge=-30, le=30)] = Field(default=8, alias="snrDb")


class Scenario(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    name: Annotated[str, Field(min_length=1, max_length=80)]
    seed: int = 1
    node_count: Annotated[int, Field(ge=2, le=10)] = Field(alias="nodeCount")
    rf: RFSettings = Field(default_factory=RFSettings)
    channel: ScenarioChannel = Field(default_factory=ScenarioChannel)
    nodes: list[ScenarioNode]
    links: list[DirectedLink]
    fresh_state: bool = Field(default=True, alias="freshState")

    @model_validator(mode="after")
    def validate_graph(self) -> Scenario:
        if self.node_count != len(self.nodes):
            raise ValueError("nodeCount must equal the number of nodes")

        node_ids = [node.id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("node IDs must be unique")
        ports = [node.api_port for node in self.nodes]
        if len(ports) != len(set(ports)):
            raise ValueError("public API ports must be unique")

        known = set(node_ids)
        link_keys: set[tuple[str, str]] = set()
        for link in self.links:
            if link.from_node == link.to_node:
                raise ValueError("self-links are not allowed")
            if link.from_node not in known or link.to_node not in known:
                raise ValueError(f"link references unknown node: {link.from_node} -> {link.to_node}")
            key = (link.from_node, link.to_node)
            if key in link_keys:
                raise ValueError(f"duplicate directed link: {link.from_node} -> {link.to_node}")
            link_keys.add(key)

        missing = [
            f"{source} -> {target}"
            for source in node_ids
            for target in node_ids
            if source != target and (source, target) not in link_keys
        ]
        if missing:
            raise ValueError(f"missing directed links: {', '.join(missing)}")
        return self

    def link_map(self) -> dict[tuple[str, str], DirectedLink]:
        return {(link.from_node, link.to_node): link for link in self.links}

    def reachable_pairs(self, *, max_hops: int | None = None) -> set[tuple[str, str]]:
        """Return pairs connected by enabled links within an optional hop limit."""

        adjacency: dict[str, set[str]] = {node.id: set() for node in self.nodes}
        for link in self.links:
            if link.enabled:
                adjacency[link.from_node].add(link.to_node)

        reachable: set[tuple[str, str]] = set()
        for source in adjacency:
            pending = deque((target, 1) for target in adjacency[source])
            visited = {source}
            while pending:
                target, hops = pending.popleft()
                if target in visited:
                    continue
                visited.add(target)
                reachable.add((source, target))
                if max_hops is None or hops < max_hops:
                    pending.extend((neighbor, hops + 1) for neighbor in adjacency[target] - visited)
        return reachable


def default_nodes(node_count: int) -> list[ScenarioNode]:
    if not 2 <= node_count <= 10:
        raise ValueError("node count must be from 2 through 10")
    return [
        ScenarioNode(
            id=f"node-{index}",
            displayName=f"Node {index}",
            role=NodeRole.CLIENT,
            apiPort=PUBLIC_PORT_MIN + index - 1,
        )
        for index in range(1, node_count + 1)
    ]


def generate_links(
    node_ids: list[str], preset: TopologyPreset, *, rssi_dbm: int = -85, snr_db: float = 8
) -> list[DirectedLink]:
    links: list[DirectedLink] = []
    hub = node_ids[0]
    for source_index, source in enumerate(node_ids):
        for target_index, target in enumerate(node_ids):
            if source == target:
                continue
            enabled = False
            if preset == TopologyPreset.FULL_MESH:
                enabled = True
            elif preset == TopologyPreset.LINE:
                enabled = abs(source_index - target_index) == 1
            elif preset == TopologyPreset.STAR:
                enabled = source == hub or target == hub
            links.append(
                DirectedLink.model_validate(
                    {
                        "from": source,
                        "to": target,
                        "enabled": enabled,
                        "rssiDbm": rssi_dbm,
                        "snrDb": snr_db,
                    }
                )
            )
    return links


def apply_topology_preset(scenario: Scenario, preset: TopologyPreset) -> Scenario:
    enabled_by_pair = {
        (link.from_node, link.to_node): link.enabled
        for link in generate_links([node.id for node in scenario.nodes], preset)
    }
    links = [
        link.model_copy(update={"enabled": enabled_by_pair[link.from_node, link.to_node]})
        for link in scenario.links
    ]
    return scenario.model_copy(update={"links": links})


def default_scenario(node_count: int = 5) -> Scenario:
    nodes = default_nodes(node_count)
    return Scenario(
        schemaVersion=1,
        name="five-node-full-mesh" if node_count == 5 else f"{node_count}-node-full-mesh",
        seed=1,
        nodeCount=node_count,
        nodes=nodes,
        links=generate_links([node.id for node in nodes], TopologyPreset.FULL_MESH),
    )

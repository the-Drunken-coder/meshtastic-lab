from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.models import (
    DirectedLink,
    Scenario,
    TopologyPreset,
    apply_topology_preset,
    default_scenario,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def test_default_scenario_has_five_nodes_and_all_directed_links() -> None:
    scenario = default_scenario()

    assert scenario.node_count == 5
    assert len(scenario.links) == 20
    assert all(link.enabled for link in scenario.links)


def test_asymmetric_links_are_preserved() -> None:
    scenario = default_scenario(2)
    links = [
        DirectedLink(**{"from": "node-1", "to": "node-2", "enabled": True}),
        DirectedLink(**{"from": "node-2", "to": "node-1", "enabled": False}),
    ]
    asymmetric = scenario.model_copy(update={"links": links})

    assert asymmetric.link_map()["node-1", "node-2"].enabled is True
    assert asymmetric.link_map()["node-2", "node-1"].enabled is False


@pytest.mark.parametrize(
    ("preset", "enabled"),
    [
        (TopologyPreset.FULL_MESH, 12),
        (TopologyPreset.LINE, 6),
        (TopologyPreset.STAR, 6),
        (TopologyPreset.ALL_ISOLATED, 0),
    ],
)
def test_topology_preset_generation(preset: TopologyPreset, enabled: int) -> None:
    scenario = apply_topology_preset(default_scenario(4), preset)

    assert sum(link.enabled for link in scenario.links) == enabled


@pytest.mark.parametrize(
    "mutation",
    [
        {"nodeCount": 3},
        {"nodes": [{"id": "node-1", "displayName": "One", "role": "CLIENT", "apiPort": 45001}] * 2},
        {
            "links": [
                {"from": "node-1", "to": "node-1", "enabled": True, "rssiDbm": -85, "snrDb": 8}
            ]
        },
    ],
)
def test_invalid_scenario_is_rejected(mutation: dict[str, object]) -> None:
    data = default_scenario(2).model_dump(by_alias=True)
    data.update(mutation)

    with pytest.raises(ValidationError):
        Scenario.model_validate(data)


def test_duplicate_directed_link_is_rejected() -> None:
    data = default_scenario(2).model_dump(by_alias=True)
    data["links"].append(data["links"][0])

    with pytest.raises(ValidationError, match="duplicate directed link"):
        Scenario.model_validate(data)


def test_unknown_meshtastic_region_is_rejected_at_scenario_boundary() -> None:
    data = default_scenario(2).model_dump(by_alias=True)
    data["rf"]["region"] = "NOT_A_REGION"

    with pytest.raises(ValidationError, match="unsupported Meshtastic region"):
        Scenario.model_validate(data)


def test_unset_meshtastic_region_is_rejected_at_scenario_boundary() -> None:
    data = default_scenario(2).model_dump(by_alias=True)
    data["rf"]["region"] = "UNSET"

    with pytest.raises(ValidationError, match="region must be set"):
        Scenario.model_validate(data)


@pytest.mark.parametrize("node_count", [2, 3])
def test_incomplete_directed_link_matrix_is_rejected(node_count: int) -> None:
    data = default_scenario(node_count).model_dump(by_alias=True)
    missing = data["links"].pop()

    with pytest.raises(
        ValidationError,
        match=f"missing directed links: {missing['from']} -> {missing['to']}",
    ):
        Scenario.model_validate(data)


def test_bundled_scenarios_validate_and_define_complete_matrices() -> None:
    paths = list((REPOSITORY_ROOT / "scenarios").glob("*.json"))
    assert paths
    for path in paths:
        scenario = Scenario.model_validate_json(path.read_text(encoding="utf-8"))
        assert len(scenario.links) == scenario.node_count * (scenario.node_count - 1)


def test_reachable_pairs_respect_hop_limit() -> None:
    scenario = apply_topology_preset(default_scenario(3), TopologyPreset.LINE)

    assert ("node-1", "node-3") in scenario.reachable_pairs()
    assert ("node-1", "node-3") not in scenario.reachable_pairs(max_hops=1)
    assert ("node-1", "node-2") in scenario.reachable_pairs(max_hops=1)

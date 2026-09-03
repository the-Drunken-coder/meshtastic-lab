from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.provenance import BuildMetadata, load_build_metadata


def test_build_metadata_loader_tracks_the_exact_image_values(tmp_path: Path) -> None:
    path = tmp_path / "build-metadata.json"
    path.write_text(
        json.dumps(
            {
                "firmwareCommit": "firmware-new",
                "collisionPatchSha256": "patch-new",
                "firmwareBinarySha256": "binary-new",
                "buildArchitecture": "aarch64",
                "clientLibraryVersion": "2.7.11",
                "upstreamBaseImageDigest": "sha256:upstream-new",
                "meshtasticatorCommit": "simulator-new",
            }
        ),
        encoding="utf-8",
    )

    first = load_build_metadata(path)
    assert first.firmware_commit == "firmware-new"
    assert first.collision_patch_sha256 == "patch-new"
    assert first.firmware_binary_sha256 == "binary-new"
    assert first.build_architecture == "aarch64"
    assert first.upstream_base_image_digest == "sha256:upstream-new"
    assert first.meshtasticator_commit == "simulator-new"

    path.write_text(
        path.read_text(encoding="utf-8").replace("firmware-new", "firmware-rebuilt"),
        encoding="utf-8",
    )
    assert load_build_metadata(path).firmware_commit == "firmware-rebuilt"


def test_missing_build_metadata_is_explicitly_unavailable(tmp_path: Path) -> None:
    metadata = load_build_metadata(tmp_path / "missing.json")
    assert metadata == BuildMetadata.unavailable()


def test_metadata_without_simulator_revision_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "build-metadata.json"
    path.write_text(
        json.dumps(
            {
                "firmwareCommit": "firmware",
                "collisionPatchSha256": "patch",
                "firmwareBinarySha256": "binary",
                "buildArchitecture": "aarch64",
                "clientLibraryVersion": "2.7.11",
                "upstreamBaseImageDigest": "sha256:base",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match=r"meshtasticCommit|meshtasticatorCommit"):
        load_build_metadata(path)


@pytest.mark.parametrize(
    "field",
    [
        "firmware_commit",
        "collision_patch_sha256",
        "firmware_binary_sha256",
        "build_architecture",
        "client_library_version",
        "upstream_base_image_digest",
        "meshtasticator_commit",
    ],
)
@pytest.mark.parametrize("value", ["", "   ", "unavailable"])
def test_build_metadata_availability_requires_every_identity_field(
    field: str, value: str
) -> None:
    metadata = BuildMetadata(
        firmwareCommit="firmware",
        collisionPatchSha256="patch",
        firmwareBinarySha256="binary",
        buildArchitecture="aarch64",
        clientLibraryVersion="2.7.11",
        upstreamBaseImageDigest="sha256:base",
        meshtasticatorCommit="simulator",
    )

    assert metadata.available
    assert not metadata.model_copy(update={field: value}).available

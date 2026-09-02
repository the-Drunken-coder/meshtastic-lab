from __future__ import annotations

import json
from pathlib import Path

from backend.app.provenance import MESHTASTICATOR_COMMIT, BuildMetadata, load_build_metadata


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
    assert first.meshtasticator_commit == MESHTASTICATOR_COMMIT

    path.write_text(
        path.read_text(encoding="utf-8").replace("firmware-new", "firmware-rebuilt"),
        encoding="utf-8",
    )
    assert load_build_metadata(path).firmware_commit == "firmware-rebuilt"


def test_missing_build_metadata_is_explicitly_unavailable(tmp_path: Path) -> None:
    metadata = load_build_metadata(tmp_path / "missing.json")
    assert metadata == BuildMetadata.unavailable()

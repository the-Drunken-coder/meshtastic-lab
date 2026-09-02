"""Build provenance for the native firmware and simulator runtime."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

MESHTASTICATOR_COMMIT = "17ceb8231079d87b070abc6132181e4c6b20202d"
DEFAULT_METADATA_PATH = Path("/usr/share/meshtastic-lab/build-metadata.json")


class BuildMetadata(BaseModel):
    """The immutable build inputs and outputs used by a simulation."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    firmware_commit: str = Field(alias="firmwareCommit")
    collision_patch_sha256: str = Field(alias="collisionPatchSha256")
    firmware_binary_sha256: str = Field(alias="firmwareBinarySha256")
    build_architecture: str = Field(alias="buildArchitecture")
    client_library_version: str = Field(alias="clientLibraryVersion")
    upstream_base_image_digest: str = Field(alias="upstreamBaseImageDigest")
    meshtasticator_commit: str = Field(
        default=MESHTASTICATOR_COMMIT, alias="meshtasticatorCommit"
    )

    @classmethod
    def unavailable(cls) -> BuildMetadata:
        """Return an explicit local-development value when Docker metadata is absent."""

        return cls(
            firmwareCommit="unavailable",
            collisionPatchSha256="unavailable",
            firmwareBinarySha256="unavailable",
            buildArchitecture="unavailable",
            clientLibraryVersion="unavailable",
            upstreamBaseImageDigest="unavailable",
        )


def load_build_metadata(
    path: Path = DEFAULT_METADATA_PATH, *, allow_missing: bool = True
) -> BuildMetadata:
    """Load the exact metadata emitted by the firmware image build.

    Production images always carry this file. The explicit unavailable fallback
    exists only so local unit tests can instantiate the service without Docker.
    Malformed or incomplete metadata is never silently replaced.
    """

    if not path.is_file():
        if allow_missing:
            return BuildMetadata.unavailable()
        raise FileNotFoundError(path)
    return BuildMetadata.model_validate_json(path.read_text(encoding="utf-8"))

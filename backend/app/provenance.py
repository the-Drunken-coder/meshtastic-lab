"""Build provenance for the native firmware and simulator runtime."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

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
    meshtasticator_commit: str = Field(alias="meshtasticatorCommit")

    @property
    def available(self) -> bool:
        """Report whether every field needed to identify a run is usable."""

        values = (
            self.firmware_commit,
            self.collision_patch_sha256,
            self.firmware_binary_sha256,
            self.build_architecture,
            self.client_library_version,
            self.upstream_base_image_digest,
            self.meshtasticator_commit,
        )
        return all(value.strip() not in {"", "unavailable"} for value in values)

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
            meshtasticatorCommit="unavailable",
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

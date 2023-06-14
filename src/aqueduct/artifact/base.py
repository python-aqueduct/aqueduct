from .artifact import ArtifactSpec, Artifact
from .local import LocalFilesystemArtifact


def resolve_artifact_from_spec(
    spec: ArtifactSpec,
) -> Artifact | None:
    if isinstance(spec, Artifact) or spec is None:
        return spec
    elif isinstance(spec, str):
        return LocalFilesystemArtifact(spec)
    else:
        raise RuntimeError(f"Could not resolve artifact spec: {spec}")

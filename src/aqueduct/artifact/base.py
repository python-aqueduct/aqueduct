from typing import overload, Type, Optional, TypeVar

from .artifact import ArtifactSpec, Artifact, TextStreamArtifact
from .local import LocalFilesystemArtifact


_T = TypeVar("_T", bound=Artifact)


@overload
def resolve_artifact_from_spec(spec: _T) -> _T:
    ...


@overload
def resolve_artifact_from_spec(spec: str) -> LocalFilesystemArtifact:
    ...


def resolve_artifact_from_spec(
    spec: ArtifactSpec,
) -> Artifact:
    if isinstance(spec, Artifact):
        return spec
    elif isinstance(spec, str):
        return LocalFilesystemArtifact(spec)
    else:
        raise RuntimeError(f"Could not resolve artifact spec: {spec}")

"""Artifacts describe how to store the return value of a :class:`Task` inside a
:class:`Store`."""
from __future__ import annotations

import logging
from typing import TypeVar


from .artifact import Artifact, ArtifactSpec
from .inmemory import InMemoryArtifact
from .local import LocalFilesystemArtifact, LocalStoreArtifact


_logger = logging.getLogger(__name__)

T = TypeVar("T")


def resolve_artifact_from_spec(
    spec: ArtifactSpec,
) -> Artifact | None:
    if isinstance(spec, Artifact) or spec is None:
        return spec
    elif isinstance(spec, str):
        return LocalFilesystemArtifact(spec)
    else:
        raise RuntimeError(f"Could not resolve artifact spec: {spec}")


__all__ = [
    "Artifact",
    "resolve_artifact_from_spec",
    "LocalFilesystemArtifact",
    "LocalStoreArtifact",
    "InMemoryArtifact",
]

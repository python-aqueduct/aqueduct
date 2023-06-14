from __future__ import annotations

import logging
from typing import TypeVar


from .artifact import (
    Artifact,
    ArtifactSpec,
    TextStreamArtifact,
    StreamArtifact,
    TextStreamArtifactSpec,
)
from .base import resolve_artifact_from_spec
from .composite import CompositeArtifact
from .inmemory import InMemoryArtifact
from .local import LocalFilesystemArtifact, LocalStoreArtifact
from .util import artifact_report


__all__ = [
    "Artifact",
    "ArtifactSpec",
    "resolve_artifact_from_spec",
    "LocalFilesystemArtifact",
    "LocalStoreArtifact",
    "InMemoryArtifact",
    "TextStreamArtifact",
    "TextStreamArtifactSpec",
    "StreamArtifact",
]

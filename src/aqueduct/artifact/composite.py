from typing import Iterable

from .artifact import Artifact


class CompositeArtifact(Artifact):
    def __init__(self, artifacts: Iterable[Artifact]):
        self.artifacts = artifacts

    def exists(self) -> bool:
        return all([x.exists() for x in self.artifacts])

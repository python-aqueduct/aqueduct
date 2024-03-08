from typing import Sequence

from .artifact import Artifact


class CompositeArtifact(Artifact):
    """Merge multiple artifacts together. Useful if a Task wants to store
    many files."""

    def __init__(self, artifacts: Sequence[Artifact]):
        self.artifacts = artifacts

    def exists(self) -> bool:
        """Return `True` if *all* the composed artifacts exist, `False` otherwise."""
        return all([x.exists() for x in self.artifacts])

    def __repr__(self):
        inner_repr = ", ".join([repr(a) for a in self.artifacts])
        return f"CompositeArtifact([{inner_repr}])"

    def __str__(self):
        return f"CompositeArtifact(... [{len(self.artifacts)} artifacts])"

    def size(self):
        return sum([x.size() for x in self.artifacts])

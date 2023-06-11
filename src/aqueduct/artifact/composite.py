from typing import Iterable

from .artifact import Artifact


class CompositeArtifact(Artifact):
    """Merge multiple artifacts together. Useful if an :class:`IOTask` wants to store
    many files."""

    def __init__(self, artifacts: Iterable[Artifact]):
        self.artifacts = artifacts

    def exists(self) -> bool:
        """Return `True` if *all* the composed artifacts exist, `False` otherwise."""
        return all([x.exists() for x in self.artifacts])

    def __repr__(self):
        inner_repr = ", ".join([repr(a) for a in self.artifacts])
        return f"CompositeArtifact([{inner_repr}])"

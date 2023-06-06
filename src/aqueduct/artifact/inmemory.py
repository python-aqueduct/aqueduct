from typing import Any, Mapping

from .artifact import Artifact


class InMemoryArtifact(Artifact):
    def __init__(self, key: str, store: Mapping[str, Any]):
        self.store = store
        self.key = key

    def exists(self) -> bool:
        return self.key in self.store

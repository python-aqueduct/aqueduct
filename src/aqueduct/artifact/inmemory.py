from typing import Any, MutableMapping

from .artifact import Artifact


class InMemoryArtifact(Artifact):
    def __init__(self, key: str, store: MutableMapping[str, Any]):
        self.store = store
        self.key = key

    def exists(self) -> bool:
        return self.key in self.store

    def size(self) -> int:
        return 0

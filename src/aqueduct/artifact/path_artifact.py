from typing import Generic, TypeVar, Callable, cast

import pathlib

from .artifact import Artifact
from ..store import LocalFilesystemStore

T = TypeVar('T')

class PathArtifact(Artifact, Generic[T]):
    def _resolve_store(self):
        store = super()._resolve_store()

        if not isinstance(store, LocalFilesystemStore):
            raise TypeError("Path artifacts are only supported with LocalFilesystemStore")
        
        return store

    def load_from_store(self, loader: Callable[[pathlib.Path], T], deserializer: Callable[[pathlib.Path], T] | None) -> T:
        loader = deserializer if deserializer else self.deserialize

        store = cast(LocalFilesystemStore, self.store)
        return store.load_from_path(self.name, loader)
    
    def dump_to_store(self, object: T):
        store = cast(LocalFilesystemStore, self.store)
        return store.dump_to_path(self.name, object, self.serialize)
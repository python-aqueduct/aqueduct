from typing import BinaryIO, Any

import pickle

from .artifact import Artifact

class PickleArtifact(Artifact):
    """Store objects using `pickle`."""

    def deserialize(self, stream: BinaryIO) -> Any:
        return pickle.load(stream)

    def serialize(self, object: Any, stream: BinaryIO):
        return pickle.dump(object, stream)
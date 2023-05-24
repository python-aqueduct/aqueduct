import logging
import pandas as pd
import pickle

from typing import TypeVar
from typing import BinaryIO

from .store import Store

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class Artifact:
    def __init__(self, name, store: Store):
        self._name = name
        self.store = store

    @property
    def name(self):
        return self._name

    def load(self, stream: BinaryIO) -> T:
        raise NotImplementedError("Artifact must implement `load` method.")

    def dump(self, object: T, stream: BinaryIO):
        raise NotImplementedError("Artifact must implement `dump` method.")

    def load_from_store(self):
        stream = self.store.get_read_stream(self.name)
        loaded_object = self.load(stream)
        stream.close()

        return loaded_object

    def dump_to_store(self, object_to_dump):
        stream = self.store.get_write_stream(self.name)
        self.dump(object_to_dump, stream)
        stream.close()

    def exists(self) -> bool:
        return self.name in self.store


class PickleArtifact(Artifact):
    def load(self, stream: BinaryIO) -> T:
        return pickle.load(stream)

    def dump(self, object: T, stream: BinaryIO):
        return pickle.dump(object, stream)


class ParquetArtifact(Artifact):
    def load(self, stream: BinaryIO) -> T:
        return pd.read_parquet(stream)

    def dump(self, df: pd.DataFrame, stream: BinaryIO):
        return df.to_parquet(stream)

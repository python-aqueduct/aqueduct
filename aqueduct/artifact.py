"""Artifacts describe how to store the return value of a :class:`Task`."""

import abc
import logging
import pandas as pd
import pickle

from typing import Any, Generic, TypeVar
from typing import BinaryIO

from .store import Store, LocalFilesystemStore

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class Artifact(Generic[T], abc.ABC):
    """Describes how to store the return value of a :class:`Task` to a
    :class:`Store`."""

    def __init__(self, name: str, store: Store = LocalFilesystemStore()):
        self._name = name
        self.store = store

    @property
    def name(self):
        return self._name

    @abc.abstractmethod
    def load(self, stream: BinaryIO) -> T:
        raise NotImplementedError("Artifact must implement `load` method.")

    @abc.abstractmethod
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
    """Store objects using `pickle`."""

    def load(self, stream: BinaryIO) -> Any:
        return pickle.load(stream)

    def dump(self, object: Any, stream: BinaryIO):
        return pickle.dump(object, stream)


class ParquetArtifact(Artifact):
    """Store :class:`pandas.DataFrame` objects to the Parquet format using `pandas`."""

    def load(self, stream: BinaryIO) -> pd.DataFrame:
        return pd.read_parquet(stream)

    def dump(self, df: pd.DataFrame, stream: BinaryIO):
        return df.to_parquet(stream)

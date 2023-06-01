"""Artifacts describe how to store the return value of a :class:`Task` inside a
:class:`Store`."""
from __future__ import annotations

import abc
import inspect
import logging
import pickle
from typing import Any, BinaryIO, Generic, TypeAlias, TypeVar, Callable, Type

import pandas as pd

from .store import Store, get_default_store

_logger = logging.getLogger(__name__)

T = TypeVar("T")

StoreSpec: TypeAlias = Store | None


def resolve_store_from_spec(spec: StoreSpec) -> Store:
    if isinstance(spec, Store):
        return spec
    else:
        return get_default_store()


class Artifact(Generic[T], abc.ABC):
    """Describes how to store the return value of a :class:`Task` to a
    :class:`Store`."""

    def __init__(self, name: str, store: StoreSpec = None):
        self._name = name
        self.store = resolve_store_from_spec(store)

    @property
    def name(self):
        return self._name

    @abc.abstractmethod
    def deserialize(self, stream: BinaryIO) -> T:
        raise NotImplementedError("Artifact must implement `load` method.")

    @abc.abstractmethod
    def serialize(self, object: T, stream: BinaryIO):
        raise NotImplementedError("Artifact must implement `dump` method.")

    def load_from_store(self):
        _logger.info(f"Loading {self.name} from store.")
        return self.store.load_binary(self.name, self.deserialize)

    def dump_to_store(self, object_to_dump):
        _logger.info(f"Saving {self.name} to store.")
        self.store.dump_binary(self.name, object_to_dump, self.serialize)

    def exists(self) -> bool:
        return self.name in self.store

    def _resolve_store(self):
        return resolve_store_from_spec(self.store)


ArtifactSpec: TypeAlias = Artifact | str | Callable[..., Artifact] | None


def resolve_artifact_cls(signature: inspect.Signature) -> Type[Artifact]:
    if signature.return_annotation == pd.DataFrame:
        return ParquetArtifact
    else:
        return get_default_artifact_cls()


def resolve_artifact_from_spec(
    spec: ArtifactSpec,
    signature: inspect.Signature,
    *args,
    **kwargs,
) -> Artifact | None:
    if isinstance(spec, Artifact):
        return spec
    elif isinstance(spec, str):
        artifact_cls = resolve_artifact_cls(signature)
        bind = signature.bind_partial(*args, **kwargs)
        artifact_name = spec.format(**bind.arguments)
        return artifact_cls(artifact_name)
    elif callable(spec):
        return spec(*args, **kwargs)
    elif spec is None:
        return None
    else:
        raise RuntimeError(f"Could not resolve artifact spec: {spec}")


class PickleArtifact(Artifact):
    """Store objects using `pickle`."""

    def deserialize(self, stream: BinaryIO) -> Any:
        return pickle.load(stream)

    def serialize(self, object: Any, stream: BinaryIO):
        return pickle.dump(object, stream)


class ParquetArtifact(Artifact):
    """Store :class:`pandas.DataFrame` objects to the Parquet format using `pandas`."""

    def deserialize(self, stream: BinaryIO) -> pd.DataFrame:
        return pd.read_parquet(stream)

    def serialize(self, df: pd.DataFrame, stream: BinaryIO):
        return df.to_parquet(stream)


def get_default_artifact_cls():
    return PickleArtifact

from typing import Any, Callable, Type, TypeVar

import abc
import inspect
import pathlib
import pickle

import pandas as pd
import xarray as xr

from ..artifact import Artifact, InMemoryArtifact, LocalFilesystemArtifact
from .task import Task

T = TypeVar("T")

READER_OF_TYPE = {
    pd.DataFrame: pd.read_parquet,
    xr.Dataset: xr.open_dataset,
    xr.DataArray: xr.open_dataarray,
}

READER_OF_SUFFIX = {
    ".parquet": pd.read_parquet,
    ".nc": xr.open_dataset,
}


WRITERS = {
    pd.DataFrame: lambda x: x.to_parquet(x),
    xr.Dataset: xr.open_dataset,
    xr.DataArray: xr.open_dataarray,
}


def pickle_write_to_file(object: Any, path: str):
    with open(path, "wb") as f:
        pickle.dump(object, f)


def pickle_load_file(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)


DEFAULT_READER = pickle_load_file
DEFAULT_WRITER = pickle_write_to_file


def resolve_writer(t: Type[T] | None) -> Callable[[T, str], None]:
    if t is not None:
        return WRITERS.get(t, DEFAULT_WRITER)
    else:
        return DEFAULT_WRITER


def resolve_reader(t: Type[T] | None, filename: pathlib.Path) -> Callable[[str], T]:
    suffix = filename.suffix

    if t is not None:
        return READER_OF_TYPE.get(t, DEFAULT_READER)
    elif t is None and suffix:
        return READER_OF_SUFFIX.get(suffix, DEFAULT_READER)
    else:
        return DEFAULT_READER


def store_artifact(artifact: Artifact, object: Any):
    if isinstance(artifact, LocalFilesystemArtifact):
        store_artifact_filesystem(artifact, object)
    elif isinstance(artifact, InMemoryArtifact):
        store_artifact_memory(artifact, object)
    else:
        raise ValueError(f"Artifact {artifact} not supported for automatic storage.")


def store_artifact_filesystem(
    artifact: LocalFilesystemArtifact,
    object: T,
    object_type_hint: Type[T] | None = None,
):
    path = artifact.path

    writer = resolve_writer(object_type_hint)
    with path.open("wb") as f:
        writer(object, str(path))


def store_artifact_memory(artifact: InMemoryArtifact, object: Any):
    store = artifact.store
    store[artifact.key] = object


def load_artifact(artifact: Artifact, type_hint: Type | None = None) -> Any:
    if isinstance(artifact, LocalFilesystemArtifact):
        return load_artifact_filesystem(artifact, type_hint)
    elif isinstance(artifact, InMemoryArtifact):
        return load_artifact_memory(artifact)
    else:
        raise ValueError(
            f"Artifact type {artifact} not supported for automatic storage."
        )


def load_artifact_filesystem(
    artifact: LocalFilesystemArtifact, type_hint: Type | None
) -> Any:
    reader = resolve_reader(type_hint, artifact.path)

    return reader(str(artifact.path))


def load_artifact_memory(artifact: InMemoryArtifact):
    return artifact.store[artifact.key]


class PureTask(Task[T], abc.ABC):
    def __call__(self, *args, **kwargs) -> T:
        artifact = self._resolve_artifact()

        if not artifact:
            result = self.run(*args, **kwargs)
        elif artifact and artifact.exists():
            result = self.load(artifact)
        else:
            result = self.run(*args, **kwargs)
            self.save(artifact, result)

        return result

    def save(self, artifact: Artifact, object: T):
        store_artifact(artifact, object)

    def load(self, artifact: Artifact) -> T:
        type_hint = inspect.signature(self.run).return_annotation

        type_hint = None if type_hint == inspect._empty else type_hint

        return load_artifact(artifact, type_hint=type_hint)

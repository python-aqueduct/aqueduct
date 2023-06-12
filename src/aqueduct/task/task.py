from typing import Any, Callable, Type, TypeVar

import inspect
import logging
import pathlib
import pickle

import pandas as pd
import xarray as xr

from ..artifact import (
    Artifact,
    InMemoryArtifact,
    LocalFilesystemArtifact,
    CompositeArtifact,
)
from .abstract_task import AbstractTask

T = TypeVar("T")


_logger = logging.getLogger(__name__)


def write_to_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path)


def write_to_netcdf(array: xr.Dataset, path: str):
    array.to_netcdf(path)


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
    pd.DataFrame: write_to_parquet,
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
    path.parent.mkdir(parents=True, exist_ok=True)

    writer = resolve_writer(type(object))

    _logger.info(f"Writing using {writer}")

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
    elif isinstance(artifact, CompositeArtifact):
        loaded_children = []
        for a in artifact.artifacts:
            loaded_children.append(load_artifact(a))

        return loaded_children
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


class Task(AbstractTask[T]):
    """Standard implementation of :class:`AbstractTask`. When called, it returns the
    value returned by `run` as expected. The :class:`Artifact` is used to automatically
    store the value returned, according to sane default policies."""

    def __call__(self, *args, **kwargs) -> T:
        """Prepare the context, execute the `run` method, and return its result.

        If an artifact is specified, save the result before returning. If an artifact is
        specified, and the artifact exists when this is called, to not call `run`, and
        load the artifact instead.

        Returns
            The result of `run`."""
        artifact = self._resolve_artifact()

        force_run = getattr(self, "_aq_force_root", False)

        if not artifact:
            _logger.info(f"Running task {self}")
            result = self.run(*args, **kwargs)
        elif artifact and artifact.exists() and not force_run:
            _logger.info(f"Loading result of {self} from {artifact}")
            result = self.load(artifact)
        else:
            _logger.info(f"Running task {self}")
            result = self.run(*args, **kwargs)

            _logger.info(f"Saving result of {self} to {artifact}")
            self.save(artifact, result)

        return result

    def run(self, *args, **kwargs):
        pass

    def save(self, artifact: Artifact, object: T):
        """Save `object` according to the specification of `artifact`.

        When the Task is executed, this method is called to save the artifact if
        one is specified by `artifact`. Override this to implement your own storage
        behavior.

        Arguments
            artifact: The artiffact that specifies where/how to save the task result.
            object: The task result."""
        store_artifact(artifact, object)

    def load(self, artifact: Artifact) -> T:
        """Load an artifact and return it.

        If an artifact is specified, this is called to load the artifact from cache
        to avoid excecuting the `run` method. Override this to implement your own
        loading behavior.
        """
        type_hint = inspect.signature(self.run).return_annotation

        type_hint = None if type_hint == inspect._empty else type_hint

        return load_artifact(artifact, type_hint=type_hint)

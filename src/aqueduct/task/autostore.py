from typing import Any, Callable, TypeVar, Type

import logging
import pandas as pd
import xarray as xr
import pathlib
import pickle

from ..artifact import (
    Artifact,
    LocalFilesystemArtifact,
    InMemoryArtifact,
    CompositeArtifact,
)

_T = TypeVar("_T")

_logger = logging.getLogger(__name__)


def write_to_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path)


def write_to_netcdf(array: xr.Dataset | xr.DataArray, path: str):
    array.to_netcdf(path)
    array.close()


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
    xr.Dataset: write_to_netcdf,
    xr.DataArray: write_to_netcdf,
}


def pickle_write_to_file(object: Any, path: str):
    with open(path, "wb") as f:
        pickle.dump(object, f)


def pickle_load_file(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)


DEFAULT_READER = pickle_load_file
DEFAULT_WRITER = pickle_write_to_file


def resolve_writer(t: Type[_T] | None) -> Callable[[_T, str], None]:
    if t is not None and t in WRITERS:
        return WRITERS[t]
    else:
        return DEFAULT_WRITER


def resolve_reader(t: Type[_T] | None, filename: pathlib.Path) -> Callable[[str], _T]:
    suffix = filename.suffix

    if t is not None and t in READER_OF_TYPE:
        return READER_OF_TYPE[t]
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
    object: _T,
    object_type_hint: Type[_T] | None = None,
):
    path = artifact.path
    path.parent.mkdir(parents=True, exist_ok=True)

    writer = resolve_writer(type(object))

    _logger.info(f"Writing using {writer}")

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

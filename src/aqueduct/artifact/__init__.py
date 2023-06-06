"""Artifacts describe how to store the return value of a :class:`Task` inside a
:class:`Store`."""
from __future__ import annotations

import inspect
import logging
from typing import TypeVar, Type

import pandas as pd
import requests
import xarray as xr

from .artifact import Artifact, ArtifactSpec
from .dataframe import DataFrameArtifact
from .http import HTTPDownloadArtifact
from .pickle import PickleArtifact
from .xarray import XarrayArtifact

_logger = logging.getLogger(__name__)

T = TypeVar("T")


def resolve_artifact_cls(signature: inspect.Signature) -> Type[Artifact]:
    if signature.return_annotation == pd.DataFrame:
        return DataFrameArtifact
    elif signature.return_annotation == xr.Dataset:
        return XarrayArtifact
    elif signature.return_annotation == requests.Response:
        return HTTPDownloadArtifact
    else:
        return PickleArtifact


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
        return resolve_artifact_from_spec(spec(*args, **kwargs), signature, *args, **kwargs)
    elif spec is None:
        return None
    else:
        raise RuntimeError(f"Could not resolve artifact spec: {spec}")


__all__ = ["HTTPDownloadArtifact", "PickleArtifact", "DataFrameArtifact", "XarrayArtifact"]
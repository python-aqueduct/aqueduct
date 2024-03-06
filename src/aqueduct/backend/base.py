from typing import TypeAlias, cast, Literal, Mapping

import collections.abc
import hydra

from .backend import Backend
from .immediate import ImmediateBackend, ImmediateBackendDictSpec
from .concurrent import ConcurrentBackend, ConcurrentBackendDictSpec
from .dask import DaskBackend, DaskBackendDictSpec, resolve_dask_backend_dict_spec
from .multiprocessing import MultiprocessingBackend, MultiprocessingBackendDictSpec

from ..config import get_aqueduct_config


NAMES_OF_BACKENDS = {
    "immediate": ImmediateBackend,
    "concurrent": ConcurrentBackend,
    "dask": DaskBackend,
    "multiprocessing": MultiprocessingBackend,
}


BackendDictSpec: TypeAlias = Mapping[str, int | str]

BackendSpec: TypeAlias = (
    Literal["immediate", "concurrent", "dask", "dask_graph", "multiprocessing"]
    | Backend
    | BackendDictSpec
    | None
)


def resolve_backend_from_spec(spec: BackendSpec) -> Backend:
    if isinstance(spec, Backend):
        return spec
    elif isinstance(spec, collections.abc.Mapping):
        return resolve_dict_backend_spec(spec)
    elif isinstance(spec, str):
        return NAMES_OF_BACKENDS[spec]()
    elif spec is None:
        cfg = get_aqueduct_config()

        if "backend" in cfg:
            backend = cast(Backend, hydra.utils.instantiate(cfg["backend"]))
            return backend
        else:
            return ImmediateBackend()
    else:
        raise ValueError(f"Could not resolve backend from spec {spec}")


def resolve_dict_backend_spec(spec: BackendDictSpec) -> Backend:
    if spec["type"] == "dask":
        return resolve_dask_backend_dict_spec(spec)
    elif spec["type"] == "concurrent":
        return ConcurrentBackend(n_workers=int(spec["n_workers"]))
    elif spec["type"] == "immediate":
        return ImmediateBackend()
    elif spec["type"] == "multiprocessing":
        return MultiprocessingBackend(n_workers=int(spec["n_workers"]))
    else:
        raise KeyError("Unrecognized backend spec")


def get_default_backend() -> Backend:
    return resolve_backend_from_spec(None)

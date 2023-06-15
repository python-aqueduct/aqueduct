from typing import TypeAlias, Optional, cast

import hydra

from .backend import Backend
from .immediate import ImmediateBackend
from .concurrent import ConcurrentBackend
from .dask import DaskBackend

from ..config import get_aqueduct_config


BackendSpec: TypeAlias = str | Backend


NAMES_OF_BACKENDS = {
    "immediate": ImmediateBackend,
    "concurrent": ConcurrentBackend,
    "dask": DaskBackend,
}


def resolve_backend_from_spec(spec: Optional[BackendSpec]) -> Backend:
    if isinstance(spec, Backend):
        return spec
    elif isinstance(spec, str):
        return NAMES_OF_BACKENDS[spec]
    elif spec is None:
        cfg = get_aqueduct_config()

        if "backend" in cfg:
            backend = cast(Backend, hydra.utils.instantiate(cfg["backend"]))
            return backend
        else:
            return ImmediateBackend()
    else:
        raise ValueError(f"Could not resolve backend from spec {spec}")


def get_default_backend() -> Backend:
    return resolve_backend_from_spec(None)

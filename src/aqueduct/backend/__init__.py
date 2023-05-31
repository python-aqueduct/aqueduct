"""A Backend is the computing resource on which a collection of `Task` are computed.

So far, only the :class:`DaskBackend` is available."""
import hydra

from typing import TypeAlias
from .backend import Backend
from ..config import get_aqueduct_config
from .dask import DaskBackend
from .immediate import ImmediateBackend

BackendSpec: TypeAlias = Backend | None


def resolve_backend_from_spec(spec: BackendSpec) -> Backend:
    if isinstance(spec, Backend):
        return spec
    elif spec is None:
        cfg = get_aqueduct_config()

        if "backend" in cfg:
            backend = hydra.utils.instantiate(cfg["backend"])
            return backend
        else:
            return ImmediateBackend()


def get_default_backend() -> Backend:
    return resolve_backend_from_spec(None)


__all__ = ["DaskBackend", "ImmediateBackend"]

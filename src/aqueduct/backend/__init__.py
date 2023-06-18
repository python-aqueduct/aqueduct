"""A Backend is the computing resource on which a collection of `Task` are computed.

So far, only the :class:`DaskBackend` is available."""

from typing import TypeAlias, Optional, cast
from .backend import Backend
from ..config import get_aqueduct_config
from .concurrent import ConcurrentBackend
from .dask import DaskBackend
from .immediate import ImmediateBackend
from .base import resolve_backend_from_spec, BackendSpec, get_default_backend


__all__ = [
    "ConcurrentBackend",
    "DaskBackend",
    "ImmediateBackend",
    "resolve_backend_from_spec",
    "BackendSpec",
    "Backend",
    "get_default_backend",
]

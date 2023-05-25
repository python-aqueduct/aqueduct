"""A Backend is the computing resource on which a collection of `Task` are computed.

So far, only the :class:`DaskBackend` is available."""

from .dask import DaskBackend
from .immediate import ImmediateBackend

__all__ = ["DaskBackend", "ImmediateBackend"]

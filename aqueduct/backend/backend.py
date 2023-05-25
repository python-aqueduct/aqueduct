"""A Backend is the computing resource on which a collection of `Task` are computed.

So far, only the :class:`DaskBackend` is available."""

import abc

from typing import TypeVar

from ..task import Binding

T = TypeVar("T")


class Backend(abc.ABC):
    @abc.abstractmethod
    def run(self, task: Binding[T]) -> T:
        raise NotImplemented("Backend must implement run.")

import abc

from typing import TypeVar

from ..task import Binding

T = TypeVar("T")


class Backend(abc.ABC):
    @abc.abstractmethod
    def run(self, task: Binding[T]) -> T:
        """Execute a :class:`Binding` using its arguments."""
        raise NotImplemented("Backend must implement run.")

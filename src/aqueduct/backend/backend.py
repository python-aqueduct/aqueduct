import abc
from typing import TypeVar

from ..task import Task

T = TypeVar("T")


class Backend(abc.ABC):
    @abc.abstractmethod
    def run(self, task: Task[T]) -> T:
        """Execute a :class:`Task` by resolving all its requirements."""
        raise NotImplemented("Backend must implement run.")

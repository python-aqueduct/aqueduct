import abc
from typing import TypeVar, Any

from ..task import AbstractTask
from ..util import TaskTree


class Backend(abc.ABC):
    @abc.abstractmethod
    def execute(self, work: TaskTree) -> Any:
        """Execute a :class:`Task` by resolving all its requirements."""
        raise NotImplemented("Backend must implement execute.")

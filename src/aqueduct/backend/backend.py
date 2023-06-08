import abc
from typing import TypeVar

from ..task import Task


class Backend(abc.ABC):
    @abc.abstractmethod
    def run(self, task: Task):
        """Execute a :class:`Task` by resolving all its requirements."""
        raise NotImplemented("Backend must implement run.")

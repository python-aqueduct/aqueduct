import abc
from typing import TypeVar

from ..task import AbstractTask


class Backend(abc.ABC):
    @abc.abstractmethod
    def execute(self, task: AbstractTask):
        """Execute a :class:`Task` by resolving all its requirements."""
        raise NotImplemented("Backend must implement run.")

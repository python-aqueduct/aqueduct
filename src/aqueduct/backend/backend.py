import abc
from typing import TypeVar, Any, TYPE_CHECKING

from ..task import AbstractTask
from ..util import TaskTree

if TYPE_CHECKING:
    from .base import BackendSpec


class Backend(abc.ABC):
    @abc.abstractmethod
    def execute(self, work: TaskTree) -> Any:
        """Execute a :class:`Task` by resolving all its requirements."""
        raise NotImplemented("Backend must implement execute.")

    @abc.abstractmethod
    def _spec(self) -> "BackendSpec":
        raise NotImplementedError("Backend must implement BackendSpec")

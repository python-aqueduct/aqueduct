import abc
from typing import TypeVar, Any, TYPE_CHECKING, Optional

from ..task_tree import TaskTree

if TYPE_CHECKING:
    from .base import BackendSpec

AQ_CURRENT_BACKEND: Optional["Backend"] = None

class TaskException(RuntimeError):
    pass

class Backend(abc.ABC):
    @abc.abstractmethod
    def _run(self, work: TaskTree) -> Any:
        raise NotImplemented("Backend must implement _run.")

    def run(self, work: TaskTree) -> Any:
        """Execute a :class:`Task` by resolving all its requirements."""
        global AQ_CURRENT_BACKEND
        AQ_CURRENT_BACKEND = self

        result = self._run(work)
        AQ_CURRENT_BACKEND = None
        return result


    @abc.abstractmethod
    def _spec(self) -> "BackendSpec":
        raise NotImplementedError("Backend must implement BackendSpec")

    def close(self):
        pass

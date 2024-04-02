import abc
from typing import Type, Any, TYPE_CHECKING, Optional

from ..task_tree import TaskTree
from ..task import AbstractTask

if TYPE_CHECKING:
    from . import BackendSpec

AQ_CURRENT_BACKEND: Optional["Backend"] = None


class TaskException(RuntimeError):
    pass


class Backend(abc.ABC):
    @abc.abstractmethod
    def _run(self, work: TaskTree, force_tasks=None) -> Any:
        raise NotImplemented("Backend must implement _run.")

    def run(
        self, work: TaskTree, force_tasks: Optional[set[Type[AbstractTask]]] = None
    ) -> Any:
        """Execute a :class:`Task` by resolving all its requirements."""
        global AQ_CURRENT_BACKEND
        AQ_CURRENT_BACKEND = self

        result = self._run(work, force_tasks=force_tasks)
        AQ_CURRENT_BACKEND = None
        return result

    @abc.abstractmethod
    def _spec(self) -> "BackendSpec":
        raise NotImplementedError("Backend must implement BackendSpec")

    def close(self):
        pass

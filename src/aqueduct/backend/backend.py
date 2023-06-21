import abc
from typing import TypeVar, Any, TYPE_CHECKING

from ..task_tree import OptionalTaskTree

if TYPE_CHECKING:
    from .base import BackendSpec


class Backend(abc.ABC):
    @abc.abstractmethod
    def execute(self, work: OptionalTaskTree) -> Any:
        """Execute a :class:`Task` by resolving all its requirements."""
        raise NotImplemented("Backend must implement execute.")

    @abc.abstractmethod
    def _spec(self) -> "BackendSpec":
        raise NotImplementedError("Backend must implement BackendSpec")

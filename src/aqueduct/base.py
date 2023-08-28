from typing import overload, TypeVar, Any

from .backend.immediate import ImmediateBackend
from .task.abstract_task import AbstractTask
from .task_tree import TaskTree

_T = TypeVar("_T")


@overload
def run(work: AbstractTask[_T]) -> _T:
    ...


@overload
def run(work: TaskTree) -> Any:
    ...


def run(work: TaskTree | AbstractTask) -> Any:
    backend = ImmediateBackend()
    return backend.run(work)

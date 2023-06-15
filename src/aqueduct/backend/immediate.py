from typing import TypeVar, Any, cast

from .backend import Backend
from ..task import AbstractTask
from ..util import resolve_task_tree, TaskTree

T = TypeVar("T")


def execute_task(task: AbstractTask[T], requirements=None) -> T:
    if requirements is not None:
        return task(requirements)
    else:
        return task()


class ImmediateBackend(Backend):
    """Simple Backend that executes the :class:`Task` immediately, in the current
    process.

    No parallelism is involved. Useful for debugging purposes. For any form of
    parallelism, the :class:`DaskBackend` is probably more appropriate."""

    def execute(self, work: TaskTree) -> Any:
        result = resolve_task_tree(work, execute_task)
        return result

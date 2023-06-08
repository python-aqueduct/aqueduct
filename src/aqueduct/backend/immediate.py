from typing import TypeVar, Any, cast

from .backend import Backend
from ..task import AbstractTask
from ..util import resolve_task_tree

T = TypeVar("T")


def execute_task(task: AbstractTask[T], requirements=None) -> T:
    if requirements:
        return task(requirements)
    else:
        return task()


class ImmediateBackend(Backend):
    """Simple Backend that executes the :class:`Task` immediately, in the current
    process.

    No parallelism is involved. Useful for debugging purposes. For any form of
    parallelism, the :class:`DaskBackend` is probably more appropriate."""

    def execute(self, task: AbstractTask[T]) -> T:
        result = resolve_task_tree(task, execute_task)
        return cast(T, result)

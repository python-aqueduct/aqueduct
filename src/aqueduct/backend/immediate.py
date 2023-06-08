from typing import TypeVar

from .backend import Backend
from ..task import Task
from ..util import resolve_task_tree

T = TypeVar("T")


def execute_task(task: Task[T], requirements=None) -> T:
    if requirements:
        return task(requirements)
    else:
        return task()


class ImmediateBackend(Backend):
    """Simple Backend that executes the :class:`Task` immediately, in the current
    process.

    No parallelism is involved. Useful for debugging purposes. For any form of
    parallelism, the :class:`DaskBackend` is probably more appropriate."""

    def run(self, task: Task[T]) -> T:
        return resolve_task_tree(task, execute_task)

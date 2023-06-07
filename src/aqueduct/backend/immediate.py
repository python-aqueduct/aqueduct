from typing import TypeVar

from .backend import Backend
from ..task import Task
from ..util import map_task_tree

T = TypeVar("T")


def execute_task(task: Task[T]) -> T:
    requirements = task.requirements()

    if requirements is not None:
        executed_requirements = map_task_tree(requirements, execute_task)
        return task(executed_requirements)
    else:
        return task()


class ImmediateBackend(Backend):
    """Simple Backend that executes the :class:`Task` immediately, in the current
    process.

    No parallelism is involved. Useful for debugging purposes. For any form of
    parallelism, the :class:`DaskBackend` is probably more appropriate."""

    def run(self, task: Task[T]) -> T:
        return map_task_tree(task, execute_task)

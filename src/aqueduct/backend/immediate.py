from typing import TypeVar, Any, TypedDict, Literal

from .backend import Backend
from ..task import AbstractTask
from ..task.parallel_task import AbstractParallelTask
from ..task_tree import TaskTree, _resolve_task_tree

T = TypeVar("T")


class ImmediateBackendDictSpec(TypedDict):
    type: Literal["immediate"]


def execute_task(task: AbstractTask[T], requirements=None) -> T:
    if isinstance(task, AbstractParallelTask):
        return execute_parallel_task(task, requirements)
    else:
        if requirements is not None:
            return task(requirements)
        else:
            return task()


def execute_parallel_task(
    task: AbstractParallelTask[Any, Any, T], requirements=None
) -> T:
    accumulator = task.accumulator(requirements)

    def map_reduce(item, acc, requirements):
        return task.reduce(task.map(item, requirements), acc, requirements)

    for item in task.items():
        accumulator = map_reduce(item, accumulator, requirements)

    return task.post(accumulator)


class ImmediateBackend(Backend):
    """Simple Backend that executes the :class:`Task` immediately, in the current
    process.

    No parallelism is involved. Useful for debugging purposes. For any form of
    parallelism, the :class:`DaskBackend` is probably more appropriate."""

    def _run(self, work: TaskTree) -> Any:
        result = _resolve_task_tree(work, execute_task)
        return result

    def _spec(self) -> str:
        return "immediate"

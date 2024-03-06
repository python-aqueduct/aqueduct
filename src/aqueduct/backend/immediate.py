from typing import TypeVar, Any, TypedDict, Literal

import logging

from ..artifact import resolve_artifact_from_spec
from .backend import Backend
from ..task import AbstractTask
from ..task.mapreduce import AbstractMapReduceTask
from ..task_tree import TaskTree, _resolve_task_tree
from ..task.task import Task

T = TypeVar("T")

_logger = logging.getLogger(__name__)


class ImmediateBackendDictSpec(TypedDict):
    type: Literal["immediate"]


def execute_task(task: AbstractTask[T], requirements=None) -> T:
    # Check if the artifact exists and computation is needed.
    artifact_spec = task.artifact()

    force_run = getattr(task, "_aq_force_root", False)

    artifact = resolve_artifact_from_spec(artifact_spec)
    if artifact is not None and artifact.exists() and not force_run:
        _logger.info(f"Loading result of {task} from {artifact}")
        return task.load()

    # Execute task.
    _logger.info(f"Running task {task}")
    if isinstance(task, Task):
        if requirements is not None:
            task_result = task.run(requirements)
        else:
            task_result = task.run()
    elif isinstance(task, AbstractMapReduceTask):
        return execute_parallel_task(task, requirements)
    else:
        raise RuntimeError("Unhandled task type.")

    # Save task.
    if task._ALLOW_SAVE and task_result is not None:
        _logger.info(f"Saving result of {task} to {artifact}")
        task.save(task_result)

    return task_result


def execute_parallel_task(
    task: AbstractMapReduceTask[Any, Any, T], requirements=None
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

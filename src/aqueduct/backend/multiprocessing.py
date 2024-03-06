from typing import TypedDict, Literal

import functools
import multiprocessing

from . import Backend
from ..task import AbstractTask, Task
from ..task.mapreduce import AbstractMapReduceTask
from ..task_tree import TaskTree, _resolve_task_tree
from .immediate import execute_task


class MultiprocessingBackendDictSpec(TypedDict):
    type: Literal["multiprocessing"]
    n_workers: int


def call_map_fn(args):
    map_fn, item, requirements = args
    return map_fn(item, requirements)


def execute_parallel_task(pool, task: AbstractMapReduceTask, requirements=None):
    accumulator = task.accumulator(requirements)
    for mapped_item in pool.imap_unordered(
        call_map_fn, [(task.map, item, requirements) for item in task.items()]
    ):
        accumulator = task.reduce(mapped_item, accumulator, requirements)

    return task.post(accumulator, requirements)


def execution_dispatch(pool, task: AbstractTask, requirements=None):
    if isinstance(task, Task):
        return execute_task(task, requirements)
    elif isinstance(task, AbstractMapReduceTask):
        return execute_parallel_task(pool, task, requirements)
    else:
        raise RuntimeError("Unhandled task type.")


class MultiprocessingBackend(Backend):
    """Computing backend based on the `multiprocessing` module. It only parallelizes
    execution of :class:`ParallelTask` instances. For other tasks, it behaves like the
    :class:`ImmediateBackend`."""

    def __init__(self, n_workers=None):
        self.n_workers = n_workers

    def _run(self, work: TaskTree):
        with multiprocessing.Pool(processes=self.n_workers) as pool:
            resolve_fn = functools.partial(execution_dispatch, pool)
            result = _resolve_task_tree(work, resolve_fn)

        return result

    def _spec(self):
        return {"type": "multiprocessing", "n_workers": self.n_workers}

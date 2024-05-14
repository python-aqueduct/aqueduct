from typing import TypedDict, Literal, Any, TypeVar

import functools
import multiprocessing

from ..task import AbstractTask, Task
from ..task.mapreduce import AbstractMapReduceTask
from .immediate import ImmediateBackend, execute_task

_T = TypeVar("_T")


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


class MultiprocessingBackend(ImmediateBackend):
    """Computing backend based on the `multiprocessing` module. It only parallelizes
    execution of :class:`ParallelTask` instances. For other tasks, it behaves like the
    :class:`ImmediateBackend`."""

    def __init__(self, n_workers=None):
        self.n_workers = n_workers
        self.pool = multiprocessing.Pool(processes=self.n_workers)

    def execute_map_reduce_task(
        self, task: AbstractMapReduceTask[Any, Any, _T], requirements=None
    ) -> _T:
        return execute_parallel_task(self.pool, task, requirements)

    def _spec(self):
        return {"type": "multiprocessing", "n_workers": self.n_workers}

    def close(self):
        self.pool.close()

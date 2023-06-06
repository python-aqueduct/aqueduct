from typing import TypeVar

from concurrent.futures import Executor, ProcessPoolExecutor, Future, wait

import cloudpickle

from .backend import Backend
from ..task import Task
from .util import map_task_tree, TaskTreeNode, map_type_in_tree


T = TypeVar("T")


class TaskMapper:
    """Callable that maps tasks to futures. It remembers all the futures it created."""

    def __init__(self, executor: Executor):
        self.futures = []
        self.executor = executor

    def __call__(self, task: Task):
        future = task_to_future(task, self.executor)
        self.futures.append(future)
        return future


def map_future_to_result(future):
    return future.result()


def task_to_future(task: Task[T], executor: Executor) -> Future[T]:
    mapper = TaskMapper(executor)

    mapped_requirements = map_task_tree(task.requirements(), mapper)

    wait(mapper.futures)

    result_requirements = map_type_in_tree(
        mapped_requirements, Future, map_future_to_result
    )

    return executor.submit(undill_and_run, cloudpickle.dumps(task), result_requirements)


def undill_and_run(serialized_fn, *args, **kwargs):
    fn = cloudpickle.loads(serialized_fn)

    try:
        return fn(*args, **kwargs)
    except Exception as e:
        print(e)
        raise


class ConcurrentBackend(Backend):
    def __init__(self, n_workers=1):
        self.n_workers = n_workers

    def run(self, task: Task[T]) -> T:
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            future = task_to_future(task, executor)

            return future.result()

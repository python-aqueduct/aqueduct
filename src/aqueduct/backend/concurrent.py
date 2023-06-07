from typing import Any, TypeVar, Optional

from concurrent.futures import Executor, ProcessPoolExecutor, Future, wait

import cloudpickle

from .backend import Backend
from ..task import Task
from ..util import (
    map_type_in_tree,
    resolve_task_tree,
    TypeTree,
)


T = TypeVar("T")


def undill_and_run(serialized_fn, *args, **kwargs):
    fn = cloudpickle.loads(serialized_fn)

    try:
        return fn(*args, **kwargs)
    except Exception as e:
        print(e)
        raise


def task_to_future_resolve(task: Task[T], executor: Executor) -> Future[T]:
    def map_task_to_future(
        task: Task[T], requirements: Optional[TypeTree[Future]] = None
    ) -> Future[T]:
        if requirements:
            requirement_futures = []

            def acc_reqs(f: Future) -> Future:
                requirement_futures.append(f)
                return f

            def future_to_value(f: Future[T]) -> T:
                return f.result()

            map_type_in_tree(requirements, Future, acc_reqs)

            wait(requirement_futures)

            mapped_requirements = map_type_in_tree(
                requirements, Future, future_to_value
            )

            return executor.submit(
                undill_and_run, cloudpickle.dumps(task), mapped_requirements
            )
        else:
            return executor.submit(undill_and_run, cloudpickle.dumps(task))

    return resolve_task_tree(task, map_task_to_future, use_cache=True)


class ConcurrentBackend(Backend):
    def __init__(self, n_workers=1):
        self.n_workers = n_workers

    def run(self, task: Task[T]) -> T:
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            future = task_to_future_resolve(task, executor)

            return future.result()

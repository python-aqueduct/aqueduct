from concurrent.futures import Executor, Future, ProcessPoolExecutor, wait
from typing import Optional, TypeVar, Any

import cloudpickle

from ..task import AbstractTask
from ..util import TypeTree, map_type_in_tree, resolve_task_tree
from .backend import Backend

T = TypeVar("T")


def undill_and_run(serialized_fn, *args, **kwargs):
    fn = cloudpickle.loads(serialized_fn)

    try:
        return fn(*args, **kwargs)
    except Exception as e:
        raise


def task_to_future_resolve(task: AbstractTask[T], executor: Executor) -> Future[T]:
    def map_task_to_future(
        task: AbstractTask[T], requirements: Optional[TypeTree[Future]] = None
    ) -> Future[T]:
        if requirements is not None:
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

    def execute(self, task: AbstractTask[T]) -> T:
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            future = task_to_future_resolve(task, executor)

            return future.result()

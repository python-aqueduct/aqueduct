from typing import Any, Callable, List, TypeVar, TYPE_CHECKING

from concurrent.futures import Executor, ProcessPoolExecutor, Future, wait

import cloudpickle

from .backend import Backend
from .util import map_binding_tree, BindingTreeNode, map_type_in_tree

if TYPE_CHECKING:
    from ..binding import Binding

T = TypeVar('T')


class BindingMapper:
    """Callable that maps bindings to futures. It remembers all the futures it created."""
    def __init__(self, executor: Executor):
        self.futures = []
        self.executor = executor

    def __call__(self, binding):
        future = binding_to_future(binding, self.executor)
        self.futures.append(future)
        return future
    
def map_future_to_result(future):
    return future.result()

def binding_to_future(binding: "Binding[T]", executor: Executor) -> Future:
    mapper = BindingMapper(executor)

    mapped_args = map_binding_tree(binding.args, mapper)
    mapped_kwargs = map_binding_tree(binding.kwargs, mapper)

    wait(mapper.futures)

    result_args = map_type_in_tree(mapped_args, Future, map_future_to_result)
    result_kwargs = map_type_in_tree(mapped_kwargs, Future, map_future_to_result)    

    return executor.submit(undill_and_run, cloudpickle.dumps(binding.fn),  *result_args, **result_kwargs)

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

    def run(self, binding: "Binding[T]") -> T:
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            future = binding_to_future(binding, executor)

            return future.result()

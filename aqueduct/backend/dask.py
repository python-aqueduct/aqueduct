from dask.distributed import as_completed, Client, Future
from typing import Any, TypeVar

from .backend import Backend
from ..task import Binding
from .util import map_binding_tree

T = TypeVar("T")


class DaskBackend(Backend):
    def __init__(self, client: Client):
        self.client = DaskClientProxy(client)

    def run(self, binding: Binding) -> Any:
        if not isinstance(binding, Binding):
            raise ValueError("Backend can only run Binding objects.")

        graph = create_dask_graph(binding, self.client)

        for _ in as_completed(self.client.futures, raise_errors=False):
            print("Task completed.")

        return graph.result()


class DaskClientProxy:
    def __init__(self, dask_client: Client):
        self.client = dask_client
        self.futures = []

    def submit(self, fn, *args, **kwargs):
        future = self.client.submit(fn, *args, **kwargs)
        self.futures.append(future)
        return future


def binding_tree_to_graph(input, client: DaskClientProxy):
    if isinstance(input, list):
        return [binding_tree_to_graph(x, client) for x in input]
    elif isinstance(input, tuple):
        return tuple_with_bindings_to_graph(input, client)
    elif isinstance(input, dict):
        return dict_with_bindings_to_graph(input, client)
    elif isinstance(input, Binding):
        return create_dask_graph(input, client)
    else:
        # By default, return the input itself. It will be passed as an immediate argument
        # to the dask task.
        return input


def tuple_with_bindings_to_graph(input: tuple, client: DaskClientProxy) -> tuple:
    return tuple([binding_tree_to_graph(x, client) for x in input])


def dict_with_bindings_to_graph(
    input: dict[T, Any], client: DaskClientProxy
) -> dict[T, Any]:
    return {k: binding_tree_to_graph(input[k], client) for k in input}


def create_dask_graph(binding: Binding, client: DaskClientProxy) -> Future:
    def binding_to_dask_future(binding: Binding) -> Future:
        new_args = map_binding_tree(binding.args, binding_to_dask_future)
        new_kwargs = map_binding_tree(binding.kwargs, binding_to_dask_future)

        print(new_kwargs)
        future = client.submit(binding.fn, *new_args, **new_kwargs)

        return future

    future = binding_to_dask_future(binding)

    return future

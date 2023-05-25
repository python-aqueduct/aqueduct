from typing import Any, TypeVar

from dask.distributed import Future, Client, as_completed

from .task import Task, Binding


class Backend:
    def run(self, task: Task):
        raise NotImplemented("Backend must implement run.")


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


T = TypeVar("T")


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
    # Could not figure out how to tell the type annotations that this is fine and
    # the dict is mapped to a dict. So I use specific functions to map a tuple and
    # a dict here.
    new_args = tuple_with_bindings_to_graph(binding.args, client)
    new_kwargs = dict_with_bindings_to_graph(binding.kwargs, client)

    future = client.submit(binding.fn, *new_args, **new_kwargs)

    return future

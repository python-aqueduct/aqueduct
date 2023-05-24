from typing import Tuple, Dict
import dask

from .task import Task, Binding


class Backend:
    def run(self, task: Task):
        raise NotImplemented("Backend must implement run.")


class DaskBackend(Backend):
    def __init__(self, client):
        self.client = client

    def run(self, binding: Binding):
        if not isinstance(binding, Binding):
            raise ValueError("Backend can only run Binding objects.")

        graph = create_dask_graph(binding)
        return graph.compute()


def binding_to_graph(input):
    if isinstance(input, list) or isinstance(input, tuple):
        return [binding_to_graph(x) for x in input]
    elif isinstance(input, dict):
        return {k: binding_to_graph(input[k]) for k in input}
    elif isinstance(input, Binding):
        return create_dask_graph(input)
    else:
        return input


def create_dask_graph(binding: Binding):
    new_args = binding_to_graph(binding.args)
    new_kwargs = binding_to_graph(binding.kwargs)

    return dask.delayed(binding.fn)(*new_args, **new_kwargs)

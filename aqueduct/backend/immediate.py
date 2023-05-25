from .backend import Backend
from .util import map_binding_tree
from ..task import Binding

from typing import TypeVar

T = TypeVar("T")


def execute_binding(binding: Binding[T]) -> T:
    new_args = map_binding_tree(binding.args, execute_binding)
    new_kwargs = map_binding_tree(binding.kwargs, execute_binding)

    return binding.fn(*new_args, **new_kwargs)


class ImmediateBackend(Backend):
    def run(self, binding: Binding[T]) -> T:
        return map_binding_tree(binding, execute_binding)

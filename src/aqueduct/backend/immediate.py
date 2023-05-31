from typing import TypeVar, TYPE_CHECKING

from .backend import Backend
from .util import map_binding_tree

T = TypeVar("T")

if TYPE_CHECKING:
    from ..binding import Binding


def execute_binding(binding: "Binding[T]") -> T:
    new_args = map_binding_tree(binding.args, execute_binding)
    new_kwargs = map_binding_tree(binding.kwargs, execute_binding)

    return binding.fn(*new_args, **new_kwargs)


class ImmediateBackend(Backend):
    """Simple Backend that executes the :class:`Binding` immediately, in the current
    process.

    No parallelism is involved. Useful for debugging purposes. For any form of
    parallelism, the :class:`DaskBackend` is probably more appropriate."""

    def run(self, binding: "Binding[T]") -> T:
        return map_binding_tree(binding, execute_binding)

from typing import Any, Callable, TypeAlias, TypeVar

from ..task import Binding

T = TypeVar("T")
U = TypeVar("U")

BindingTreeNode: TypeAlias = Any


def map_binding_tree(tree: BindingTreeNode, fn: Callable[[Binding], Any]) -> Any:
    """Recursively explore data structures containing Bindings, and map all Bindings
    found using `fn`.

    Arguments:
        tree: The data structure to recursively explore. fn: The function to map a
        Binding to something else.

    Returns:
        An equivalent data structure, where all the bindings have been mapped using
        `fn`."""
    if isinstance(tree, list):
        return map_binding_list(tree, fn)
    elif isinstance(tree, tuple):
        return map_binding_tuple(tree, fn)
    elif isinstance(tree, dict):
        return map_binding_dict(tree, fn)
    elif isinstance(tree, Binding):
        return fn(tree)
    else:
        # By default, return the input itself. It will be passed as an immediate argument
        # to the dask task.
        return tree


def map_binding_tuple(input: tuple, fn) -> tuple:
    return tuple([map_binding_tree(x, fn) for x in input])


def map_binding_list(input: list, fn) -> list:
    return [map_binding_tree(x, fn) for x in input]


def map_binding_dict(input: dict[T, Any], fn) -> dict[T, Any]:
    return {k: map_binding_tree(input[k], fn) for k in input}

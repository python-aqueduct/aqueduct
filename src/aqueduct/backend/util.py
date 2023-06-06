from typing import Any, Callable, TypeAlias, TypeVar, Type

from ..task import Task

T = TypeVar("T")

TaskTreeNode: TypeAlias = Any


def map_type_in_tree(tree, type: Type[T], fn: Callable[[T], Any]) -> Any:
    """Recursively explore data structures containing T, and map all T
    found using `fn`.

    Arguments:
        tree: The data structure to recursively explore. fn: The function to map a
        T to something else.

    Returns:
        An equivalent data structure, where all the T have been mapped using
        `fn`."""
    if isinstance(tree, list):
        return map_type_in_list(tree, type, fn)
    elif isinstance(tree, tuple):
        return map_type_in_tuple(tree, type, fn)
    elif isinstance(tree, dict):
        return map_type_in_dict(tree, type, fn)
    elif isinstance(tree, type):
        return fn(tree)
    else:
        # By default, return the input itself. It will be passed as an immediate argument
        # to the dask task.
        return tree


def map_type_in_tuple(input: tuple, type, fn) -> tuple:
    return tuple([map_type_in_tree(x, type, fn) for x in input])


def map_type_in_list(input: list, type, fn) -> list:
    return [map_type_in_tree(x, type, fn) for x in input]


def map_type_in_dict(input: dict[T, Any], type, fn) -> dict[T, Any]:
    return {k: map_type_in_tree(input[k], type, fn) for k in input}


def map_task_tree(tree: TaskTreeNode, fn: Callable[[Task], Any]) -> Any:
    """Recursively explore data structures containing Tasks, and map all Tasks
    found using `fn`.

    Arguments:
        tree: The data structure to recursively explore. fn: The function to map a
        Task to something else.

    Returns:
        An equivalent data structure, where all the tasks have been mapped using
        `fn`."""
    return map_type_in_tree(tree, Task, fn)

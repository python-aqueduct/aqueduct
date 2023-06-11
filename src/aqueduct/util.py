import importlib
import inspect
from typing import Any, Callable, Optional, TypeVar, TypeAlias, Union, Type

from .task import AbstractTask

T = TypeVar("T")
U = TypeVar("U")

TypeTree: TypeAlias = Union[
    list["TypeTree[T]"], tuple["TypeTree[T]"], dict[str, "TypeTree[T]"], T
]

TaskTree: TypeAlias = TypeTree[AbstractTask]


def map_type_in_tree(
    tree: TypeTree[T], type: Type[T], fn: Callable[[T], U]
) -> TypeTree[U]:
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
        raise TypeError("Unexpected type inside Tree")


def map_type_in_tuple(input: tuple, type, fn) -> tuple:
    return tuple([map_type_in_tree(x, type, fn) for x in input])


def map_type_in_list(input: list, type, fn) -> list:
    return [map_type_in_tree(x, type, fn) for x in input]


def map_type_in_dict(input: dict[T, Any], type, fn) -> dict[T, Any]:
    return {k: map_type_in_tree(input[k], type, fn) for k in input}


def map_task_tree(
    tree: TypeTree[AbstractTask], fn: Callable[[AbstractTask], U]
) -> TypeTree[U]:
    """Recursively explore data structures containing Tasks, and map all Tasks
    found using `fn`.

    Arguments:
        tree: The data structure to recursively explore. fn: The function to map a
        Task to something else.

    Returns:
        An equivalent data structure, where all the tasks have been mapped using
        `fn`."""
    return map_type_in_tree(tree, AbstractTask, fn)


def count_tasks_to_run(task: AbstractTask, remove_duplicates=True, use_cache=True):
    tasks_by_type = {}

    def handle_one_task(task: AbstractTask, *args, **kwargs):
        if not use_cache or not task.is_cached():
            task_type = task.__class__.__qualname__
            list_of_type = tasks_by_type.get(task_type, [])
            list_of_type.append(task)
            tasks_by_type[task_type] = list_of_type

        return task

    resolve_task_tree(task, handle_one_task, use_cache=use_cache)

    if remove_duplicates:
        counts = {
            k: len(set([x._unique_key() for x in tasks_by_type[k]]))
            for k in tasks_by_type
        }
    else:
        counts = {k: len(tasks_by_type[k]) for k in tasks_by_type}

    return counts


def resolve_task_tree(
    task: AbstractTask,
    fn: Callable[..., T],
    use_cache=True,
) -> T:
    """Apply function fn on all Task objects encountered while resolving the
    dependencies of `task`. If a Task has a cached value, do not expand its
    requirements, and map it immediately. Otherwise, map the task and provide its
    requirements are arguments."""

    def mapper(task: AbstractTask) -> T:
        requirements = task._resolve_requirements()

        if requirements is None:
            return fn(task)
        else:
            mapped_requirements = map_task_tree(requirements, mapper)
            return fn(task, mapped_requirements)

    return mapper(task)


def tasks_in_module(
    module_name: str, package: Optional[str] = None
) -> set[Type[AbstractTask]]:
    mod = importlib.import_module(module_name, package=package)
    members = mod.__dict__

    tasks = []
    for k in members:
        if inspect.isclass(members[k]) and issubclass(members[k], AbstractTask):
            if inspect.getmodule(members[k]) == mod:
                tasks.append(members[k])

    return set(tasks)

import importlib
import math
import inspect
import tqdm
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    TypeAlias,
    Union,
    Type,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from .task import AbstractTask

T = TypeVar("T")
U = TypeVar("U")

TypeTree: TypeAlias = Union[
    list["TypeTree[T]"], tuple["TypeTree[T]"], dict[str, "TypeTree[T]"], T, None
]

TaskTree: TypeAlias = TypeTree["AbstractTask"]


def map_type_in_tree(
    tree: TypeTree[T],
    type: Type[T],
    fn: Callable[[T], U],
    on_expand: Optional[Callable[[int], None]] = None,
    on_resolve: Optional[Callable[[], None]] = None,
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
        if on_expand is not None:
            on_expand(len(tree))
        return map_type_in_list(tree, type, fn)
    elif isinstance(tree, tuple):
        if on_expand is not None:
            on_expand(len(tree))
        return map_type_in_tuple(tree, type, fn)
    elif isinstance(tree, dict):
        if on_expand is not None:
            on_expand(len(tree))
        return map_type_in_dict(tree, type, fn)
    elif isinstance(tree, type):
        to_return = fn(tree)
        if on_resolve:
            on_resolve()
        return to_return
    else:
        raise TypeError("Unexpected type inside Tree")


def map_type_in_tuple(input: tuple, type, fn) -> tuple:
    return tuple([map_type_in_tree(x, type, fn) for x in input])


def map_type_in_list(input: list, type, fn) -> list:
    return [map_type_in_tree(x, type, fn) for x in input]


def map_type_in_dict(input: dict[T, Any], type, fn) -> dict[T, Any]:
    return {k: map_type_in_tree(input[k], type, fn) for k in input}


def map_task_tree(
    tree: TypeTree["AbstractTask"],
    fn: Callable[["AbstractTask"], U],
    on_expand: Optional[Callable[[int], None]] = None,
    on_resolve: Optional[Callable[[], None]] = None,
) -> TypeTree[U]:
    """Recursively explore data structures containing Tasks, and map all Tasks
    found using `fn`.

    Arguments:
        tree: The data structure to recursively explore. fn: The function to map a
        Task to something else.

    Returns:
        An equivalent data structure, where all the tasks have been mapped using
        `fn`."""
    from .task import AbstractTask

    return map_type_in_tree(
        tree, AbstractTask, fn, on_expand=on_expand, on_resolve=on_resolve
    )


def count_tasks_to_run(
    task: "AbstractTask", remove_duplicates=True, ignore_cache=False
):
    tasks_by_type = {}

    def handle_one_task(task: "AbstractTask", *args, **kwargs):
        if ignore_cache or not task.is_cached():
            task_type = task.__class__.__qualname__
            list_of_type = tasks_by_type.get(task_type, [])
            list_of_type.append(task)
            tasks_by_type[task_type] = list_of_type

        return task

    resolve_task_tree(task, handle_one_task, ignore_cache=ignore_cache)

    if remove_duplicates:
        counts = {
            k: len(set([x._unique_key() for x in tasks_by_type[k]]))
            for k in tasks_by_type
        }
    else:
        counts = {k: len(tasks_by_type[k]) for k in tasks_by_type}

    return counts


def task_to_result(task: "AbstractTask[T]") -> T:
    requirements = task._resolve_requirements()

    if requirements is None:
        return task()
    else:
        mapped_requirements = map_task_tree(requirements, task_to_result)
        return task(mapped_requirements)


def resolve_task_tree(
    work: TaskTree,
    fn: Callable,
    ignore_cache=False,
) -> Any:
    """Apply function fn on all Task objects encountered while resolving the
    dependencies of `task`. If a Task has a cached value, do not expand its
    requirements, and map it immediately. Otherwise, map the task and provide its
    requirements are arguments."""

    with tqdm.tqdm(total=0) as pbar:

        def on_expand(n):
            pbar.total += n

        def on_resolve():
            pbar.update(1)

        def mapper(task: "AbstractTask") -> Any:
            requirements = task._resolve_requirements(ignore_cache=ignore_cache)

            if requirements is None:
                to_return = fn(task)
            else:
                mapped_requirements = map_task_tree(
                    requirements, mapper, on_expand=on_expand, on_resolve=on_resolve
                )
                to_return = fn(task, mapped_requirements)

            pbar.update(1)

            return to_return

        return map_task_tree(work, mapper)


def tasks_in_module(
    module_name: str, package: Optional[str] = None
) -> set[Type["AbstractTask"]]:
    from .task import AbstractTask

    mod = importlib.import_module(module_name, package=package)
    members = mod.__dict__

    tasks = []
    for k in members:
        if inspect.isclass(members[k]) and issubclass(members[k], AbstractTask):
            if inspect.getmodule(members[k]) == mod:
                tasks.append(members[k])

    return set(tasks)


def convert_size(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

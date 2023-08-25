"""New task tree resolution module, with more options. Should gradually replace the 
functions in .util."""


from typing import (
    Any,
    Callable,
    Iterable,
    overload,
    Optional,
    Type,
    TypeAlias,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

if TYPE_CHECKING:
    from .task import AbstractTask

_K = TypeVar("_K")
_A = TypeVar("_A")
_T = TypeVar("_T")
_U = TypeVar("_U")


TypeTree: TypeAlias = (
    list["TypeTree[_T]"] | tuple["TypeTree[_T]"] | dict[Any, "TypeTree[_T]"] | _T | None
)

TaskTree: TypeAlias = TypeTree["AbstractTask"]


def reduce_type_in_tree(
    tree: TypeTree[_T],
    type: Type[_T],
    reduce_fn: Callable[[_T, _A], _A],
    acc: _A,
) -> _A:
    if isinstance(tree, (list, tuple)):
        for x in tree:
            acc = reduce_type_in_tree(x, type, reduce_fn, acc)
        return acc
    elif isinstance(tree, dict):
        for v in tree.values():
            acc = reduce_type_in_tree(v, type, reduce_fn, acc)
        return acc
    elif isinstance(tree, type):
        return reduce_fn(tree, acc)
    elif tree is None:
        return acc
    else:
        raise ValueError(f"Could not handle tree node {tree}.")


def gather_tasks_in_tree(tree: TypeTree["AbstractTask"]) -> list["AbstractTask"]:
    from .task import AbstractTask

    def reduce_fn(lhs: AbstractTask, acc: list[AbstractTask]) -> list[AbstractTask]:
        acc.append(lhs)
        return acc

    return reduce_type_in_tree(tree, AbstractTask, reduce_fn, [])


OneExpandCallback: TypeAlias = Callable[[list | tuple | dict], None]


@overload
def _map_type_in_tree(
    tree: list,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[OneExpandCallback] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> list:
    ...


@overload
def _map_type_in_tree(
    tree: dict[_K, Any],
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[OneExpandCallback] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> dict[_K, Any]:
    ...


@overload
def _map_type_in_tree(
    tree: tuple,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[OneExpandCallback] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> tuple:
    ...


@overload
def _map_type_in_tree(
    tree: _T,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[OneExpandCallback] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> _U:
    ...


@overload
def _map_type_in_tree(
    tree: Any,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[OneExpandCallback] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> Any:
    ...


def _map_type_in_tree(
    tree: Any,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[OneExpandCallback] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> Any:
    if isinstance(tree, (list, tuple, dict)):
        if on_expand is not None:
            on_expand(tree)

        kwargs = {
            "on_expand": on_expand,
            "before_map": before_map,
            "after_map": after_map,
        }

        if isinstance(tree, list):
            return [_map_type_in_tree(x, type, map_fn, **kwargs) for x in tree]
        elif isinstance(tree, tuple):
            return tuple([_map_type_in_tree(x, type, map_fn, **kwargs) for x in tree])
        elif isinstance(tree, dict):
            return {k: _map_type_in_tree(tree[k], type, map_fn, **kwargs) for k in tree}
    elif isinstance(tree, type):
        if before_map:
            before_map(tree)
        mapped = map_fn(tree)
        if after_map:
            after_map(mapped)

        return mapped
    else:
        raise ValueError(f"Could not handle tree node {tree}.")


def _map_tasks_in_tree(
    tree: TypeTree["AbstractTask"],
    map_fn: Callable[["AbstractTask"], _U],
    **kwargs,
) -> TypeTree[_U]:
    from .task import AbstractTask

    return _map_type_in_tree(tree, AbstractTask, map_fn, **kwargs)


def _resolve_task_tree(
    work: TaskTree,
    fn: Callable,
    ignore_cache=False,
    **kwargs,
) -> Any:
    def mapper(task: "AbstractTask") -> Any:
        requirements = task._resolve_requirements(ignore_cache=ignore_cache)

        if requirements is None:
            to_return = fn(task)
        else:
            mapped_requirements = _map_tasks_in_tree(requirements, mapper, **kwargs)
            to_return = fn(task, mapped_requirements)

        return to_return

    return _map_tasks_in_tree(work, mapper, **kwargs)

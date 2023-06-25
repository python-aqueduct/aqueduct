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
)

if TYPE_CHECKING:
    from .task import AbstractTask

_K = TypeVar("_K")
_T = TypeVar("_T")
_U = TypeVar("_U")

TypeTree: TypeAlias = (
    list["TypeTree[_T]"] | tuple["TypeTree[_T]"] | dict[Any, "TypeTree[_T]"] | _T | None
)

TaskTree: TypeAlias = TypeTree["AbstractTask"]


def _reduce_type_in_tree(
    tree: TypeTree[_T],
    type: Type[_T],
    reduce_fn: Callable[[_T, _U], _U],
    acc: _U,
) -> _U:
    if isinstance(tree, (list, tuple)):
        for x in tree:
            acc = _reduce_type_in_tree(x, type, reduce_fn, acc)
        return acc
    elif isinstance(tree, dict):
        for v in tree.values():
            acc = _reduce_type_in_tree(v, type, reduce_fn, acc)
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

    return _reduce_type_in_tree(tree, AbstractTask, reduce_fn, [])


def _map_type_in_list(
    tree: list[TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U], **kwargs
) -> list[TypeTree[_U]]:
    return [_map_type_in_tree(x, type, map_fn, **kwargs) for x in tree]


def _map_type_in_tuple(
    tree: tuple[TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U], **kwargs
) -> tuple[TypeTree[_U]]:
    return tuple([_map_type_in_tree(x, type, map_fn, **kwargs) for x in tree])


def _map_type_in_dict(
    tree: dict[_K, TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U], **kwargs
) -> dict[_K, TypeTree[_U]]:
    return {k: _map_type_in_tree(tree[k], type, map_fn, **kwargs) for k in tree}


@overload
def _map_type_in_tree(
    tree: list[TypeTree[_T]],
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[Callable[[Iterable[TypeTree[_T]]], None]] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> list[TypeTree[_U]]:
    ...


@overload
def _map_type_in_tree(
    tree: dict[_K, TypeTree[_T]],
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[Callable[[Iterable[TypeTree[_T]]], None]] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> dict[_K, TypeTree[_U]]:
    ...


@overload
def _map_type_in_tree(
    tree: tuple[TypeTree[_T]],
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[Callable[[Iterable[TypeTree[_T]]], None]] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> tuple[TypeTree[_U]]:
    ...


@overload
def _map_type_in_tree(
    tree: _T,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[Callable[[Iterable[TypeTree[_T]]], None]] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> _U:
    ...


@overload
def _map_type_in_tree(
    tree: None,
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[Callable[[Iterable[TypeTree[_T]]], None]] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> None:
    ...


def _map_type_in_tree(
    tree: TypeTree[_T],
    type: Type[_T],
    map_fn: Callable[[_T], _U],
    on_expand: Optional[Callable[[Iterable[TypeTree[_T]]], None]] = None,
    before_map: Optional[Callable[[_T], None]] = None,
    after_map: Optional[Callable[[_U], None]] = None,
) -> TypeTree[_U]:
    if isinstance(tree, (list, tuple, dict)):
        if on_expand is not None:
            on_expand(tree)

        kwargs = {
            "on_expand": on_expand,
            "before_map": before_map,
            "after_map": after_map,
        }

        if isinstance(tree, list):
            return _map_type_in_list(tree, type, map_fn, **kwargs)
        elif isinstance(tree, tuple):
            return _map_type_in_tuple(tree, type, map_fn, **kwargs)
        elif isinstance(tree, dict):
            return _map_type_in_dict(tree, type, map_fn, **kwargs)
    elif isinstance(tree, type):
        if before_map:
            before_map(tree)
        mapped = map_fn(tree)
        if after_map:
            after_map(mapped)

        return mapped
    elif tree is None:
        return None
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

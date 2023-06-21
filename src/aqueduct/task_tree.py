"""New task tree resolution module, with more options. Should gradually replace the 
functions in .util."""


from typing import (
    Any,
    Callable,
    cast,
    overload,
    Type,
    TypeAlias,
    TypeVar,
    Optional,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from .task import AbstractTask

_K = TypeVar("_K")
_T = TypeVar("_T")
_U = TypeVar("_U")

TypeTree: TypeAlias = (
    list["TypeTree[_T]"] | tuple["TypeTree[_T]"] | dict[Any, "TypeTree[_T]"] | _T
)

TaskTree: TypeAlias = TypeTree["AbstractTask"]
OptionalTaskTree: TypeAlias = TypeTree["AbstractTask" | None]


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
        breakpoint()
        return reduce_fn(tree, acc)
    else:
        raise ValueError(f"Could not handle tree node {tree}.")


def gather_tasks_in_tree(tree: TypeTree["AbstractTask"]) -> list["AbstractTask"]:
    from .task import AbstractTask

    def reduce_fn(lhs: AbstractTask, acc: list[AbstractTask]) -> list[AbstractTask]:
        acc.append(lhs)
        return acc

    return _reduce_type_in_tree(tree, AbstractTask, reduce_fn, [])


def _map_type_in_list(
    tree: list[TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U]
) -> list[TypeTree[_U]]:
    return [_map_type_in_tree(x, type, map_fn) for x in tree]


def _map_type_in_tuple(
    tree: tuple[TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U]
) -> tuple[TypeTree[_U]]:
    return tuple([_map_type_in_tree(x, type, map_fn) for x in tree])


def _map_type_in_dict(
    tree: dict[_K, TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U]
) -> dict[_K, TypeTree[_U]]:
    return {k: _map_type_in_tree(tree[k], type, map_fn) for k in tree}


@overload
def _map_type_in_tree(
    tree: list[TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U]
) -> list[TypeTree[_U]]:
    ...


@overload
def _map_type_in_tree(
    tree: dict[_K, TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U]
) -> dict[_K, TypeTree[_U]]:
    ...


@overload
def _map_type_in_tree(
    tree: tuple[TypeTree[_T]], type: Type[_T], map_fn: Callable[[_T], _U]
) -> tuple[TypeTree[_U]]:
    ...


@overload
def _map_type_in_tree(tree: _T, type: Type[_T], map_fn: Callable[[_T], _U]) -> _U:
    ...


def _map_type_in_tree(
    tree: TypeTree[_T], type: Type[_T], map_fn: Callable[[_T], _U]
) -> TypeTree[_U]:
    if isinstance(tree, list):
        return _map_type_in_list(tree, type, map_fn)
    if isinstance(tree, tuple):
        return _map_type_in_tuple(tree, type, map_fn)
    elif isinstance(tree, dict):
        return _map_type_in_dict(tree, type, map_fn)
    elif isinstance(tree, type):
        return map_fn(tree)
    else:
        raise ValueError(f"Could not handle tree node {tree}.")


def map_tasks_in_tree(
    tree: TypeTree["AbstractTask"], map_fn: Callable[["AbstractTask"], _U]
) -> TypeTree[_U]:
    from .task import AbstractTask

    return _map_type_in_tree(tree, AbstractTask, map_fn)

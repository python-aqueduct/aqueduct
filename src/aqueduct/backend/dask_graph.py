from typing import (
    Any,
    TypeVar,
    Optional,
    TYPE_CHECKING,
    TypedDict,
    Literal,
    TypeAlias,
    Hashable,
    Mapping,
)

import logging
import omegaconf as oc

from dask.optimization import fuse, inline_functions
from dask.distributed import Client, LocalCluster
import dask.utils

from ..config import set_config, get_config
from .backend import Backend
from ..task import AbstractTask
from ..task_tree import (
    TaskTree,
)

_T = TypeVar("_T")

_logger = logging.getLogger(__name__)

DaskComputation: TypeAlias = Any  # Must be any because of literal types.
DaskGraph: TypeAlias = Mapping[Hashable, DaskComputation]


class DaskGraphBackendDictSpec(TypedDict):
    type: Literal["dask_graph"]
    address: str


class DaskGraphBackend(Backend):
    """Execute :class:`Task` on a Dask cluster.

    Arguments:
        client (`dask.Client`): Client pointing to the desired Dask cluster."""

    def __init__(self, client: Optional[Client] = None):
        if client is None:
            cluster = LocalCluster()
            self.client = cluster.get_client()
        else:
            self.client = client

    def execute(self, task: TaskTree):
        _logger.info("Computing Dask graph...")
        computation, graph = add_work_to_dask_graph(task, {}, ignore_cache=False)
        _logger.info(f"Dask Graph has {len(graph)} unique tasks.")

        _logger.info("Optimizing graph...")
        optimized, dependencies = fuse(graph, keys=computation)
        optimized = inline_functions(optimized, computation, [tuple])
        _logger.info(f"Optimized graph has {len(optimized)} tasks.")

        return self.client.get(optimized, computation)

    def _scheduler_address(self):
        return self.client.scheduler_info()["address"]

    def _spec(self) -> DaskGraphBackendDictSpec:
        if isinstance(self.client, LocalCluster):
            return {
                "type": "dask_graph",
                "n_workers": len(self.client.scheduler_info()["workers"]),
            }
        else:
            return {"type": "dask_graph", "address": self._scheduler_address()}

    def __str__(self):
        return f"DaskGraphBackend"


def wrap_task(cfg: oc.DictConfig, task: AbstractTask, *args, **kwargs):
    set_config(cfg)
    return task(*args, **kwargs)


def resolve_dask_graph_backend_dict_spec(
    spec: DaskGraphBackendDictSpec,
) -> DaskGraphBackend:
    client = resolve_client_from_dict_spec(spec)
    return DaskGraphBackend(client)


def resolve_client_from_dict_spec(spec: DaskGraphBackendDictSpec):
    match spec:
        case {"address": str(address)}:
            return Client(address)
        case {"n_workers": int(n_workers)}:
            return Client(LocalCluster(n_workers=n_workers))
        case _:
            raise ValueError("Could not parse Dask backend specification.")


def add_task_to_dask_graph(
    task: AbstractTask, graph: DaskGraph, ignore_cache: bool = False
) -> tuple[str, DaskGraph]:
    task_key = task._unique_key()

    if task_key in graph:
        return task_key, graph

    requirements = task._resolve_requirements(ignore_cache=ignore_cache)

    current_cfg = get_config()

    if requirements is None:
        graph[task_key] = (wrap_task, current_cfg, task)
    else:
        computation, graph = add_work_to_dask_graph(
            requirements, graph, ignore_cache=ignore_cache
        )
        graph[task_key] = (wrap_task, current_cfg, task, computation)

    return task_key, graph


def add_list_to_dask_graph(
    work: list, graph: DaskGraph, ignore_cache: bool = False
) -> tuple[list[str | list], DaskGraph]:
    new_list = []
    for x in work:
        computation, graph = add_work_to_dask_graph(x, graph, ignore_cache=ignore_cache)
        new_list.append(computation)

    return new_list, graph


def add_tuple_to_dask_graph(
    work: tuple, graph: DaskGraph, ignore_cache: bool = False
) -> tuple[DaskComputation, DaskGraph]:
    work_as_list = list(work)

    computation, graph = add_list_to_dask_graph(
        work_as_list, graph, ignore_cache=ignore_cache
    )

    return (tuple, computation), graph


def add_work_to_dask_graph(
    work: list | AbstractTask, graph: DaskGraph, ignore_cache: bool = False
) -> tuple[DaskComputation, DaskGraph]:
    if isinstance(work, list):
        computation, graph = add_list_to_dask_graph(
            work, graph, ignore_cache=ignore_cache
        )
    elif isinstance(work, AbstractTask):
        computation, graph = add_task_to_dask_graph(
            work, graph, ignore_cache=ignore_cache
        )
    elif isinstance(work, tuple):
        computation, graph = add_tuple_to_dask_graph(
            work, graph, ignore_cache=ignore_cache
        )
    else:
        raise RuntimeError("Unhandled type when adding work to dask graph.")

    return computation, graph

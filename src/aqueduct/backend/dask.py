from typing import (
    Any,
    cast,
    Optional,
    TypeAlias,
    Hashable,
    Mapping,
    MutableMapping,
)

import logging
import omegaconf as oc

from dask.optimization import fuse, inline_functions
from dask.distributed import Client, LocalCluster

import aqueduct.backend.backend

from ..config import set_config, get_config
from .backend import Backend
from ..task import AbstractTask
from ..task.parallel_task import AbstractParallelTask
from ..task_tree import (
    TaskTree,
)

_logger = logging.getLogger(__name__)

DaskComputation: TypeAlias = Any  # Must be any because of literal types.
DaskGraph: TypeAlias = MutableMapping[Hashable, DaskComputation]

DaskBackendDictSpec: TypeAlias = Mapping[str, int | str]


class DaskBackend(Backend):
    """Execute :class:`Task` on a Dask cluster.

    Arguments:
        client (`dask.Client`): Client pointing to the desired Dask cluster."""

    def __init__(self, client: Optional[Client] = None):
        if client is None:
            cluster = LocalCluster()
            self.client = cluster.get_client()
        else:
            self.client = client

    def _run(self, task: TaskTree):
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

    def _spec(self) -> DaskBackendDictSpec:
        scheduler_address = cast(str, self._scheduler_address())
        return {"type": "dask", "address": scheduler_address}

    def __str__(self):
        return f"DaskBackend"


def wrap_task(
    backend_spec: DaskBackendDictSpec,
    cfg: oc.DictConfig,
    task: AbstractTask,
    *args,
    **kwargs,
):
    aqueduct.backend.backend.AQ_CURRENT_BACKEND = resolve_dask_backend_dict_spec(
        backend_spec
    )
    set_config(cfg)
    return task(*args, **kwargs)


def resolve_dask_backend_dict_spec(
    spec: DaskBackendDictSpec,
) -> DaskBackend:
    client = resolve_client_from_dict_spec(spec)
    return DaskBackend(client)


def resolve_client_from_dict_spec(spec: DaskBackendDictSpec):
    match spec:
        case {"type": "dask", "address": str(address)}:
            return Client(address)
        case {"type": "dask", "n_workers": int(n_workers)}:
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

    if aqueduct.backend.backend.AQ_CURRENT_BACKEND is None:
        raise RuntimeError("No backend set.")

    if requirements is None:
        graph[task_key] = (
            wrap_task,
            aqueduct.backend.backend.AQ_CURRENT_BACKEND._spec(),
            current_cfg,
            task,
        )
    else:
        computation, graph = add_work_to_dask_graph(
            requirements, graph, ignore_cache=ignore_cache
        )
        graph[task_key] = (
            wrap_task,
            aqueduct.backend.backend.AQ_CURRENT_BACKEND._spec(),
            current_cfg,
            task,
            computation,
        )

    return task_key, graph


def add_parallel_task_to_dask_graph(
    parallel_task: AbstractParallelTask, graph, ignore_cache=False
):
    """Expand all the work in a parallel task and add it to the graph."""
    if aqueduct.backend.backend.AQ_CURRENT_BACKEND is None:
        raise RuntimeError("No backend set.")

    # Resolve requirements.
    requirements = parallel_task._resolve_requirements(ignore_cache=ignore_cache)
    if requirements is not None:
        requirements_key, graph = add_work_to_dask_graph(
            requirements, graph, ignore_cache=ignore_cache
        )
    else:
        requirements_key = None

    # Define a map_reduce function to avoid adding unnecessary complexity to the graph.
    def map_reduce(item, acc, requirements):
        return parallel_task.reduce(
            parallel_task.map(item, requirements), acc, requirements
        )

    # Gather task context.
    base_task_key = parallel_task._unique_key()
    current_cfg = get_config()

    # Insert accumulator into graph.
    accumulator_key = f"{base_task_key}_accumulator"
    graph[accumulator_key] = (parallel_task.accumulator, requirements_key)

    # Expand items and perform map reduce.
    items_list = list(parallel_task.items())
    for idx, item in enumerate(items_list):
        reduce_task_key = f"{base_task_key}_reduce_{idx}"

        # We use a binary tree to make a balanced reduce.
        left_child_idx = idx * 2 + 1
        right_child_idx = idx * 2 + 2

        if left_child_idx < len(items_list):
            left_child_key = f"{base_task_key}_reduce_{left_child_idx}"
        else:
            left_child_key = accumulator_key

        if right_child_idx < len(items_list):
            right_child_key = f"{base_task_key}_reduce_{right_child_idx}"
        else:
            right_child_key = accumulator_key

        # Add children together.
        children_reduce_work_unit = (
            wrap_task,
            aqueduct.backend.backend.AQ_CURRENT_BACKEND._spec(),
            current_cfg,
            parallel_task.reduce,
            left_child_key,
            right_child_key,
            requirements_key,
        )

        # Add reduce of children with map of current node.
        self_reduce_work_unit = (
            wrap_task,
            aqueduct.backend.backend.AQ_CURRENT_BACKEND._spec(),
            current_cfg,
            map_reduce,
            item,
            children_reduce_work_unit,
            requirements_key,
        )

        # Add all the tasks implied by this in the graph.
        graph[reduce_task_key] = self_reduce_work_unit

    if len(items_list) > 0:
        root_reduce_key = f"{base_task_key}_reduce_{0}"
    else:
        root_reduce_key = accumulator_key

    return root_reduce_key, graph


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


def add_dict_to_dask_graph(
    work: dict, graph: DaskGraph, ignore_cache: bool = False
) -> tuple[DaskComputation, DaskGraph]:
    work_as_list = list(work.values())

    computation, graph = add_list_to_dask_graph(
        work_as_list, graph, ignore_cache=ignore_cache
    )

    def rebuild_dict(mapped_values):
        return {k: v for k, v in zip(work.keys(), mapped_values)}

    return (rebuild_dict, computation), graph


def add_work_to_dask_graph(
    work: TaskTree, graph: DaskGraph, ignore_cache: bool = False
) -> tuple[DaskComputation, DaskGraph]:
    if isinstance(work, list):
        computation, graph = add_list_to_dask_graph(
            work, graph, ignore_cache=ignore_cache
        )
    elif isinstance(work, AbstractParallelTask):
        computation, graph = add_parallel_task_to_dask_graph(
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
    elif isinstance(work, dict):
        computation, graph = add_dict_to_dask_graph(
            work, graph, ignore_cache=ignore_cache
        )
    else:
        raise RuntimeError("Unhandled type when adding work to dask graph.")

    return computation, graph

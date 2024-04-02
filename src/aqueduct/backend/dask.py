import functools
from typing import (
    Any,
    Callable,
    Type,
    cast,
    Optional,
    TypeAlias,
    Hashable,
    Mapping,
    MutableMapping,
)

import logging
import omegaconf as oc

import aqueduct.backend.backend
from dask.optimization import fuse, inline_functions
from dask.distributed import Client, LocalCluster
from aqueduct.artifact.base import resolve_artifact_from_spec

from aqueduct.backend.immediate import ImmediateBackend

from ..config import set_config, get_config
from ..task import AbstractTask
from ..task.task import Task
from ..task.mapreduce import AbstractMapReduceTask
from ..task_tree import (
    TaskTree,
)

_logger = logging.getLogger(__name__)

DaskComputation: TypeAlias = Any  # Must be any because of literal types.
DaskGraph: TypeAlias = MutableMapping[Hashable, DaskComputation]

DaskBackendDictSpec: TypeAlias = Mapping[str, int | str]


class DaskBackend(ImmediateBackend):
    """Execute :class:`Task` on a Dask cluster.

    Arguments:
        client (`dask.Client`): Client pointing to the desired Dask cluster."""

    def __init__(self, client: Optional[Client] = None):
        if client is None:
            cluster = LocalCluster()
            self.client = cluster.get_client()
        else:
            self.client = client

    def _run(self, task: TaskTree, force_tasks: set[Type[AbstractTask]] = set()):
        _logger.info("Computing Dask graph...")
        computation, graph = add_work_to_dask_graph(
            task, {}, self._spec(), ignore_cache=False, force_tasks=force_tasks
        )
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

    def close(self):
        self.client.close()


def wrap_in_context(
    cfg: oc.DictConfig,
    backend_spec,
    fn: Callable,
    *args,
    **kwargs,
):
    """When executing a function on remote, make sure to set up the aqueduct context
    before."""
    aqueduct.backend.backend.AQ_CURRENT_BACKEND = resolve_dask_backend_dict_spec(
        backend_spec
    )
    set_config(cfg)
    return fn(*args, **kwargs)


def build_dask_task(
    cfg: oc.DictConfig, backend_spec: DaskBackendDictSpec, fn: Callable, *args
) -> tuple:
    """Utility function so that we can have type hints when building dask task tuples."""
    return (wrap_in_context, cfg, backend_spec, fn, *args)


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
            return Client(LocalCluster(processes=n_workers))
        case _:
            raise ValueError("Could not parse Dask backend specification.")


def save_and_return(task, result):
    task.save(result)
    return result


def add_task_to_dask_graph(
    task: AbstractTask,
    graph: DaskGraph,
    backend_spec: DaskBackendDictSpec,
    ignore_cache: bool = False,
    force_tasks: set[Type[AbstractTask]] = set(),
) -> tuple[str, DaskGraph]:
    # Check if task is already in graph.
    task_key = task._unique_key()
    if task_key in graph:
        return task_key, graph

    # Prepare context.
    current_cfg = get_config()

    # Check if the artifact exists and computation is needed.
    artifact_spec = task.artifact()
    is_force_task = [issubclass(task.__class__, c) for x in force_tasks]
    force_run = getattr(task, "_aq_force_root", False) or is_force_task

    artifact = resolve_artifact_from_spec(artifact_spec)
    if (
        artifact is not None
        and artifact.exists()
        and not force_run
        and task.AQ_AUTOLOAD
    ):
        # The task was in cache, we can just load it.
        _logger.info(f"Loading result of {task} from {artifact}")
        graph[task_key] = build_dask_task(current_cfg, backend_spec, task.load)
        final_key = task_key

    else:
        # We need to execute the task.
        if isinstance(task, Task):
            task_key, graph = add_single_task_to_dask_graph(
                task, graph, backend_spec, ignore_cache=ignore_cache
            )
        elif isinstance(task, AbstractMapReduceTask):
            task_key, graph = add_parallel_task_to_dask_graph(
                task, graph, backend_spec, ignore_cache=ignore_cache
            )
        else:
            raise RuntimeError("Unhandled type when adding task to dask graph.")

        if artifact is not None and task.AQ_AUTOSAVE:
            # Put a new task in front of the original, which saves the result before returning it.
            final_key = task_key + "_save_and_return"
            graph[final_key] = build_dask_task(
                current_cfg,
                backend_spec,
                functools.partial(save_and_return, task),
                task_key,
            )
        else:
            final_key = task_key

    return final_key, graph


def add_single_task_to_dask_graph(task: Task, graph, backend_spec, ignore_cache=False):
    task_key = task._unique_key()

    requirements = task._resolve_requirements(ignore_cache=ignore_cache)

    current_cfg = get_config()

    if requirements is None:
        graph[task_key] = build_dask_task(
            current_cfg,
            backend_spec,
            task,
        )
    else:
        computation, graph = add_work_to_dask_graph(
            requirements, graph, backend_spec, ignore_cache=ignore_cache
        )
        graph[task_key] = build_dask_task(current_cfg, backend_spec, task, computation)

    return task_key, graph


def add_parallel_task_to_dask_graph(
    parallel_task: AbstractMapReduceTask, graph, backend_spec, ignore_cache=False
):
    """Expand all the work in a parallel task and add it to the graph."""
    # Resolve requirements.
    requirements = parallel_task._resolve_requirements(ignore_cache=ignore_cache)
    if requirements is not None:
        requirements_key, graph = add_work_to_dask_graph(
            requirements, graph, backend_spec, ignore_cache=ignore_cache
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
        children_reduce_work_unit = build_dask_task(
            current_cfg,
            backend_spec,
            parallel_task.reduce,
            left_child_key,
            right_child_key,
            requirements_key,
        )

        # Add reduce of children with map of current node.
        self_reduce_work_unit = build_dask_task(
            current_cfg,
            backend_spec,
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

    post_task_key = f"{base_task_key}"
    graph[post_task_key] = build_dask_task(
        current_cfg,
        backend_spec,
        parallel_task.post,
        root_reduce_key,
        requirements_key,
    )

    return post_task_key, graph


def add_list_to_dask_graph(
    work: list, graph: DaskGraph, backend_spec, ignore_cache: bool = False
) -> tuple[list[str | list], DaskGraph]:
    new_list = []
    for x in work:
        computation, graph = add_work_to_dask_graph(
            x, graph, backend_spec, ignore_cache=ignore_cache
        )
        new_list.append(computation)

    return new_list, graph


def add_tuple_to_dask_graph(
    work: tuple, graph: DaskGraph, backend_spec, ignore_cache: bool = False
) -> tuple[DaskComputation, DaskGraph]:
    work_as_list = list(work)

    computation, graph = add_list_to_dask_graph(
        work_as_list, graph, backend_spec, ignore_cache=ignore_cache
    )

    return (tuple, computation), graph


def add_dict_to_dask_graph(
    work: dict, graph: DaskGraph, backend_spec, ignore_cache: bool = False
) -> tuple[DaskComputation, DaskGraph]:
    work_as_list = list(work.values())

    computation, graph = add_list_to_dask_graph(
        work_as_list, graph, backend_spec, ignore_cache=ignore_cache
    )

    def rebuild_dict(mapped_values):
        return {k: v for k, v in zip(work.keys(), mapped_values)}

    return (rebuild_dict, computation), graph


def add_work_to_dask_graph(
    work: TaskTree,
    graph: DaskGraph,
    backend_spec: DaskBackendDictSpec,
    ignore_cache: bool = False,
    force_tasks: set[Type[AbstractTask]] = set(),
) -> tuple[DaskComputation, DaskGraph]:

    if isinstance(work, AbstractTask):
        computation, graph = add_task_to_dask_graph(
            work,
            graph,
            backend_spec,
            ignore_cache=ignore_cache,
            force_tasks=force_tasks,
        )
    elif isinstance(work, list):
        computation, graph = add_list_to_dask_graph(
            work, graph, backend_spec, ignore_cache=ignore_cache
        )
    elif isinstance(work, tuple):
        computation, graph = add_tuple_to_dask_graph(
            work, graph, backend_spec, ignore_cache=ignore_cache
        )
    elif isinstance(work, dict):
        computation, graph = add_dict_to_dask_graph(
            work, graph, backend_spec, ignore_cache=ignore_cache
        )
    else:
        raise RuntimeError("Unhandled type when adding work to dask graph.")

    return computation, graph

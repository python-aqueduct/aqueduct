from typing import (
    Any,
    TypeVar,
    Mapping,
    Optional,
    TYPE_CHECKING,
    TypedDict,
    Literal,
)

import logging
import threading
import time
import tqdm
import queue

from dask.distributed import Client, Future, as_completed, LocalCluster, wait

from ..config import set_config, get_config
from .backend import Backend
from ..task import AbstractTask
from ..task_tree import TaskTree, _map_type_in_tree, _resolve_task_tree, TypeTree

if TYPE_CHECKING:
    from .base import BackendSpec

_T = TypeVar("_T")

_logger = logging.getLogger(__name__)


class DaskBackendDictSpec(TypedDict):
    type: Literal["dask"]
    address: str


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

        self.unique_keys_done = set()
        self.unique_keys_submitted = set()

    def execute(self, task: TaskTree):
        with tqdm.tqdm() as pbar:
            _logger.info("Creating graph...")

            def on_future(future):
                self.unique_keys_submitted.add(future.key)
                try:
                    future.add_done_callback(on_future_done)
                except RuntimeError as e:
                    breakpoint()
                pbar.reset(total=len(self.unique_keys_submitted))
                pbar.update(len(self.unique_keys_done))
                pbar.refresh()

            def on_future_done(future):
                self.unique_keys_done.add(future.key)
                pbar.reset(len(self.unique_keys_submitted))
                pbar.update(len(self.unique_keys_done))
                pbar.refresh()

            graph, futures_by_key = create_dask_graph(
                task,
                self.client,
                backend_spec=self._spec(),
                on_future=on_future,
            )

            _logger.info(f"Submitted {len(futures_by_key)} unique tasks.")

            value = compute_dask_graph(graph)

            return value

    def _scheduler_address(self):
        return self.client.scheduler_info()["address"]

    def _spec(self) -> DaskBackendDictSpec:
        return {"type": "dask", "address": self._scheduler_address()}

    def __str__(self):
        return f"DaskBackend(scheduler={self._scheduler_address()})"


def wrap_task(cfg: Mapping[str, Any], task: AbstractTask, *args, **kwargs):
    set_config(cfg)
    return task(*args, **kwargs)


def create_dask_graph(
    task: TaskTree,
    client: Client,
    backend_spec: "Optional[BackendSpec]",
    on_task=None,
    on_future=None,
) -> tuple[TypeTree[Future], dict[str, Future]]:
    task_keys = {}

    def gather_key(task: Future):
        task_keys[task.key] = task

    def task_to_dask_future(task: AbstractTask, requirements=None) -> Future:
        cfg = get_config()

        if requirements is not None:
            future = client.submit(
                wrap_task,
                cfg,
                task,
                requirements,
                backend_spec=backend_spec,
                key=task._unique_key(),
            )
        else:
            future = client.submit(
                wrap_task, cfg, task, backend_spec=backend_spec, key=task._unique_key()
            )

        if on_future is not None:
            on_future(future)

        return future

    future = _resolve_task_tree(
        task, task_to_dask_future, after_map=gather_key, before_map=on_task
    )

    return future, task_keys


def compute_dask_graph(graph):
    """This fetches the first layer of futures but does not resolve dependencies. Useful
    to get the list of tasks we need to wait on to execute another task."""
    computed = _map_type_in_tree(graph, Future, dask_future_to_result)
    return computed


def resolve_dask_dict_backend_spec(spec: DaskBackendDictSpec) -> DaskBackend:
    return DaskBackend(Client(address=spec.get("address", None)))


def dask_future_to_result(future: Future):
    return future.result()

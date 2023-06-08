from typing import Any, TypeVar, cast, Mapping

import dask.distributed
import logging
import tqdm

from dask.distributed import Client, Future, as_completed, SpecCluster

from ..config import set_config, get_config
from .backend import Backend
from ..task import Task
from ..util import resolve_task_tree


T = TypeVar("T")

_logger = logging.getLogger(__name__)


class DaskBackend(Backend):
    """Execute :class:`Task` on a Dask cluster.

    Arguments:
        client (`dask.Client`): Client pointing to the desired Dask cluster."""

    def __init__(self, cluster=None, jobs=None):
        self.cluster = cluster
        self.jobs = jobs
        self.client = DaskClientProxy(Client(address=cluster))

    def run(self, task: Task[T]) -> T:
        if self.jobs:
            cluster = cast(SpecCluster, self.cluster)
            _logger.info("Scaling cluster...")
            cluster.scale(jobs=self.jobs)  # type: ignore

        _logger.info("Creating graph...")
        graph = create_dask_graph(task, self.client)

        for _ in tqdm.tqdm(
            as_completed(self.client.futures, raise_errors=False), desc="Dask jobs"
        ):
            pass

        return cast(T, graph.result())


class DaskClientProxy:
    """Proxy to the Dask Client that remembers all the calls to submit and holds on to
    the future they return."""

    def __init__(self, dask_client: Client):
        self.client = dask_client
        self.futures = []
        _logger.info(
            f"Connected to Dask client with Dashboard Link: {dask_client.dashboard_link}"
        )

    def submit(self, fn, *args, **kwargs):
        future = self.client.submit(fn, *args, **kwargs)
        self.futures.append(future)
        return future


def wrap_task(cfg: Mapping[str, Any], task: Task, *args, **kwargs):
    set_config(cfg)
    return task(*args, **kwargs)


def create_dask_graph(task: Task, client: DaskClientProxy) -> Future:
    def task_to_dask_future(task: Task, requirements=None) -> Future:
        cfg = get_config()

        if requirements is not None:
            future = client.submit(
                wrap_task, cfg, task, requirements, key=task._unique_key()
            )
        else:
            future = client.submit(wrap_task, cfg, task, key=task._unique_key())

        return future

    future = resolve_task_tree(task, task_to_dask_future, use_cache=True)

    return future

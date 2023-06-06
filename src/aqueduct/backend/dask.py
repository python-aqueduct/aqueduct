from typing import Any, TypeVar, TYPE_CHECKING

import logging

from dask.distributed import Client, Future, as_completed

from .backend import Backend
from ..task import Task
from .util import map_task_tree


T = TypeVar("T")

_logger = logging.getLogger(__name__)


class DaskBackend(Backend):
    """Execute :class:`Task` on a Dask cluster.

    Arguments:
        client (`dask.Client`): Client pointing to the desired Dask cluster."""

    def __init__(self, cluster):
        self.client = DaskClientProxy(Client(cluster))

    def run(self, task: Task[T]) -> Any:
        _logger.info("Creating graph...")
        graph = create_dask_graph(task, self.client)

        for _ in as_completed(self.client.futures, raise_errors=False):
            print("Task completed.")

        return graph.result()


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


def create_dask_graph(task: Task, client: DaskClientProxy) -> Future:
    def task_to_dask_future(task: Task) -> Future:
        requirements = map_task_tree(task.requirements())
        new_args = map_task_tree(requirements, task_to_dask_future)

        _logger.debug(f"Submitting task {task}")
        future = client.submit(task, requirements)

        return future

    future = task_to_dask_future(task)

    return future

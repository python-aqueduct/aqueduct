from typing import Any, TypeVar, TYPE_CHECKING

import logging

from dask.distributed import Client, Future, as_completed

from .backend import Backend
from .util import map_binding_tree

if TYPE_CHECKING:
    from ..binding import Binding

T = TypeVar("T")

_logger = logging.getLogger(__name__)


class DaskBackend(Backend):
    """Execute :class:`Task` on a Dask cluster.

    Arguments:
        client (`dask.Client`): Client pointing to the desired Dask cluster."""

    def __init__(self, cluster):
        self.client = DaskClientProxy(Client(cluster))

    def run(self, binding: "Binding") -> Any:
        _logger.info("Creating graph...")
        graph = create_dask_graph(binding, self.client)

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


def create_dask_graph(binding: "Binding", client: DaskClientProxy) -> Future:
    def binding_to_dask_future(binding: "Binding") -> Future:
        new_args = map_binding_tree(binding.args, binding_to_dask_future)
        new_kwargs = map_binding_tree(binding.kwargs, binding_to_dask_future)

        _logger.debug(f"Submitting task {binding}")
        future = client.submit(binding.fn, *new_args, **new_kwargs)

        return future

    future = binding_to_dask_future(binding)

    return future

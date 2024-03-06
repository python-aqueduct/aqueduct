from typing import Generic, Type, TypeVar, Optional

import logging


import pandas as pd
import xarray as xr

from aqueduct.task.mapreduce import MapReduceTask

from .abstract_task import AbstractTask

_T = TypeVar("_T")


_logger = logging.getLogger(__name__)


class Task(MapReduceTask, Generic[_T]):
    """Pose a regular task as a specialized version of a parallel task."""

    def __call__(self, requirements=None) -> _T:
        return self.run(requirements)

    def items(self, requirements=None) -> list:
        """The list of input items to be processed in parallel."""
        return []

    def run(self, requirements=None) -> _T:
        raise NotImplementedError()

    def accumulator(self, requirements=None) -> None:
        """Base accumulator with which the reduce operation is initialized.
        Override this method to provide a custom accumulator.

        Returns:
            Base accumulator with which the reduce operation is initialized.
            Defaults to an empty list."""
        return None

    def post(self, acc: None, requirements=None) -> _T:
        return self.run(requirements)

"""Tasks are the basic building block of Aqueduct. A Task is a unit of work. It has 
four main features:

- A `run` method which describes the work to be carried out.
- A `requirements` method which describes what Tasks should be carried out before this 
  one.
- An `artifact` method which is useful to avoid useless computations if the Task result
  is cached.
- A `CONFIG` variable that describes how to fetch the configuration options of the task.

The two concrete types of tasks which you should subclass are :class:`Task` and
:class:`IOTask`."""

from .abstract_task import AbstractTask
from .aggregate import AggregateTask
from .apply import apply
from .extract_artifact import as_artifact
from .inline import inline
from .io_task import IOTask
from .notebook import NotebookTask
from .repeater import RepeaterTask
from .task import Task
from .parallel_task import ParallelTask

__all__ = [
    "AbstractTask",
    "AggregateTask",
    "apply",
    "as_artifact",
    "Task",
    "inline",
    "IOTask",
    "NotebookTask",
    "RepeaterTask",
    "ParallelTask",
]

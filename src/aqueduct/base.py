from typing import overload, TypeVar, Any

from .backend.immediate import ImmediateBackend
from .task.abstract_task import AbstractTask
from .task_tree import TaskTree

_T = TypeVar("_T")


@overload
def run(work: AbstractTask[_T]) -> _T:
    ...


@overload
def run(work: TaskTree) -> Any:
    ...


def run(work: TaskTree | AbstractTask, force_root: bool=False) -> Any:
    """Run a task or a data structure of tasks. The data structure can be built using
    regular Python `dict`, `list`, and `tuple` objects.
    
    Args:
        work: The task or task tree to run.
        force_root: If True, force the root task to run even its artifact is present
            on disk.
            
    Returns:
        If `work` is a single task, return the result of that task. That is, return 
        the output of the task's `run` method. 
        
        If `work` is a task tree, return the same data structure, but with the tasks
        replace by their results.
    """
    
    force_tasks = set()
    if force_root:
        force_tasks.add(work.__class__)


    backend = ImmediateBackend()
    return backend.run(work)

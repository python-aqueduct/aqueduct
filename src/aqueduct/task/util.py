from typing import Any

from .abstract_task import AbstractTask
from ..util import map_task_tree
from ..backend import ImmediateBackend


def execute_task(task: AbstractTask):
    if task.is_cached():
        return task()

    requirements = task._resolve_requirements()

    if requirements is None:
        return task()
    else:
        mapped_requirements = map_task_tree(requirements, execute_task)
        return task(mapped_requirements)


def compute_requirements(task: AbstractTask) -> Any:
    requirements = task._resolve_requirements()

    b = ImmediateBackend()
    b.execute()

    return map_task_tree(requirements, execute_task)
